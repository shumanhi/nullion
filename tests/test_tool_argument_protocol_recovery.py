"""Malformed model arguments remain protocol failures, never action inputs."""
import json
from types import SimpleNamespace
import pytest
from nullion import model_clients


@pytest.mark.parametrize('raw', ['{"command":"draw(', '[1,2]', 'null', '"text"', 4, None])
def test_parser_rejects_non_object_and_truncated_arguments_without_leaking_content(raw):
    with pytest.raises(model_clients.ToolArgumentsDecodeError) as error:
        model_clients.parse_tool_arguments(raw)
    assert str(raw) not in str(error.value)
    assert error.value.diagnostic['code'] in {'invalid_json','expected_object'}


@pytest.mark.parametrize('value', [{'command':'print("a")\n# 日本語'}, {'arguments':[1,2]}, {'content':'مرحبا'*3000}])
def test_complete_objects_keep_exact_values(value):
    assert model_clients.parse_tool_arguments(json.dumps(value)) == value
    assert model_clients.parse_tool_arguments(value) == value


@pytest.mark.parametrize('mode', ['completed_invalid','unfinished_valid','incomplete_valid'])
def test_real_stream_adapter_preserves_typed_argument_failure(monkeypatch, mode):
    from tests.test_terminal_model_response_contract import provider, event
    raw='{"command":"unfinished' if mode=='completed_invalid' else '{"command":"true"}'
    if mode=='completed_invalid':
        streams=[[event('response.completed',{'status':'completed','output':[
            {'type':'function_call','call_id':'one','name':'terminal_exec','arguments':raw}]})]]
    elif mode=='unfinished_valid':
        streams=[['data:'+json.dumps({'type':'response.output_item.added','output_index':0,'item':{
            'type':'function_call','call_id':'one','name':'terminal_exec','arguments':raw}}),'data: [DONE]']]
    else:
        streams=[[event('response.incomplete',{'status':'incomplete','incomplete_details':{'reason':'max_output_tokens'},'output':[
            {'type':'function_call','call_id':'one','name':'terminal_exec','arguments':raw}]})]]
    client, calls=provider(monkeypatch,streams)
    result=client.create(messages=[],tools=[])
    block=result['content'][0]
    assert block['input_error']['code'] in {'invalid_json','unfinished_tool_call','incomplete_response'}
    assert len(calls)==1
    if mode=='completed_invalid':
        assert block['input']=={}
        assert set(block['input_error'])=={'code','argument_chars','argument_digest','offset'}


def test_schema_preflight_precedes_quality_review(monkeypatch):
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass
    registry=ToolRegistry()
    def forbidden(invocation): pytest.fail('Malformed arguments executed')
    registry.register(ToolSpec('store_record','Store selected data',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,3,
        input_schema={'type':'object','properties':{'value':{'type':'string'}},'required':['value']}),forbidden)
    calls=[]
    class Model:
        def create(self,**kw):
            calls.append(kw)
            if 'Workflow action preflight.' in str(kw.get('system') or ''):
                pytest.fail('Invalid schema reached semantic reviewer')
            if len(calls)==1:return {'stop_reason':'tool_use','content':[{'type':'tool_use','id':'bad','name':'store_record','input':{}}]}
            return {'stop_reason':'end_turn','content':[{'type':'text','text':'The required value was missing.'}]}
    result=AgentOrchestrator(model_client=Model()).run_turn(conversation_id='test:protocol',user_message='記録して',
        principal_id='operator',conversation_history=[],tool_registry=registry,
        policy_store=None,approval_store=None,max_iterations=2)
    assert result.tool_results[0].output['reason']=='invalid_tool_arguments'
    assert result.tool_results[0].output['executed'] is False


def test_scope_hint_cannot_replace_grounded_partial_answer():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    from nullion.tools import ToolResult
    scope=ToolResult('s','request_tool_scope','completed',{'capabilities':['image_generation'],
        'available_tools':['file_write','terminal_exec'], 'unavailable_capabilities':['image_generation']})
    answer='The selected appointment is confirmed. The generated PNG is unavailable because its tool call was incomplete.'
    assert sanitize_user_visible_reply(user_message='調べて .png にして',reply=answer,tool_results=[scope],source='agent')==answer


@pytest.mark.parametrize('completion', [None,{'status':'incomplete','reason':'max_output_tokens'}])
def test_langchain_preserves_protocol_errors_and_never_executes_default_call(completion):
    from langchain_core.messages import AIMessage,HumanMessage,SystemMessage,ToolMessage
    from langchain_core.tools import tool
    from langgraph.prebuilt import ToolNode
    from langgraph.graph import StateGraph,MessagesState,START,END
    from nullion.langchain_adapters import nullion_client_as_langchain_chat_model,_nullion_messages_from_langchain_messages
    executed=[]
    @tool
    def mutate(value: str='default') -> str:
        """Store a selected value."""
        executed.append(value)
        return value
    class Client:
        def create(self,**kw):
            return {'stop_reason':'tool_use','completion':completion,
                'content':[{'type':'tool_use','id':'bad','name':'mutate','input':{},
                    **({'input_error':{'code':'invalid_json'}} if completion is None else {})}]}
    message=nullion_client_as_langchain_chat_model(Client()).bind_tools([mutate]).invoke([HumanMessage(content='保存')])
    assert message.tool_calls==[]
    assert len(message.invalid_tool_calls)==1
    assert 'not executed' in message.content
    graph=StateGraph(MessagesState)
    graph.add_node('tools',ToolNode([mutate]))
    graph.add_edge(START,'tools');graph.add_edge('tools',END)
    graph.compile().invoke({'messages':[message]})
    assert executed==[]
    converted,_=_nullion_messages_from_langchain_messages([message],
        message_types=(AIMessage,HumanMessage,SystemMessage,ToolMessage))
    assert 'protocol correction' in json.dumps(converted)


def test_langchain_mixed_response_executes_only_the_complete_call():
    from langchain_core.tools import tool
    from langgraph.prebuilt import ToolNode
    from langgraph.graph import StateGraph,MessagesState,START,END
    from nullion.langchain_adapters import _chat_result_from_nullion_response
    executed=[]
    @tool
    def store(value: str='default') -> str:
        """Store the selected value."""
        executed.append(value)
        return value
    message=_chat_result_from_nullion_response({'content':[
        {'type':'tool_use','id':'bad','name':'store','input':{},'input_error':{'code':'unfinished_tool_call'}},
        {'type':'tool_use','id':'good','name':'store','input':{'value':'selected'}}]}).generations[0].message
    graph=StateGraph(MessagesState);graph.add_node('tools',ToolNode([store]))
    graph.add_edge(START,'tools');graph.add_edge('tools',END)
    result=graph.compile().invoke({'messages':[message]})
    assert executed==['selected']
    assert result['messages'][-1].tool_call_id=='good'
    assert len(message.invalid_tool_calls)==1
