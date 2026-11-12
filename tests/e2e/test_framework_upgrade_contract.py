"""Real framework graphs and Nullion adapters; fake only the remote model."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import time
import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Container framework E2E')


@pytest.mark.asyncio
@pytest.mark.parametrize('engine', ['langchain', 'deepagents'])
@pytest.mark.parametrize('prompt,answer', [
    ('Create the structured note, read it back, and finish.', 'Saved and read back.'),
    ('Crea la nota estructurada, vuelve a leerla y termina.', 'Guardado y leído.'),
    ('構造化メモを作成し、読み直して完了してください。', '保存して読み直しました。'),
])
async def test_graph_tool_arguments_and_checkpoint_survive_real_adapters(tmp_path, engine, prompt, answer):
    from langchain_core.messages import HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from nullion.langchain_adapters import nullion_client_as_langchain_chat_model, nullion_tools_as_langchain_tools
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
                               _build_file_write_handler, _build_file_read_handler)
    path = tmp_path / 'cedar-note.txt'
    content = 'Cedar — café / 山\n{"count": 17, "quoted": "a\\b"}\n'
    results = []
    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    for name, effect, handler in [
        ('file_write', ToolSideEffectClass.WRITE, _build_file_write_handler(tmp_path, allowed_roots=[tmp_path])),
        ('file_read', ToolSideEffectClass.READ, _build_file_read_handler(tmp_path, allowed_roots=[tmp_path])),
    ]:
        registry.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 5), handler)

    class Provider:
        calls = 0
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=True)
            if protocol_reply is not None: return protocol_reply
            self.calls += 1
            assert self.calls <= 3, 'Framework restarted completed tool work'
            names = {row['name'] for row in kwargs['tools']}
            assert {'file_write', 'file_read'} <= names
            if self.calls == 1:
                return {'content': [{'type': 'tool_use', 'id': 'write-cedar', 'name': 'file_write',
                                     'input': {'path': str(path), 'content': content}}]}
            blocks = [block for message in kwargs['messages'] for block in message.get('content', [])
                      if isinstance(block, dict)]
            write_calls = [block for block in blocks if block.get('type') == 'tool_use' and block.get('id') == 'write-cedar']
            assert len(write_calls) == 1 and write_calls[0]['input']['content'] == content
            assert any(block.get('type') == 'tool_result' and block.get('tool_use_id') == 'write-cedar' for block in blocks)
            if self.calls == 2:
                return {'content': [{'type': 'tool_use', 'id': 'read-cedar', 'name': 'file_read', 'input': {'path': str(path)}}]}
            assert any(block.get('type') == 'tool_result' and block.get('tool_use_id') == 'read-cedar'
                       and 'Cedar' in json.dumps(block, ensure_ascii=False) for block in blocks)
            return {'content': [{'type': 'text', 'text': answer}]}

    provider = Provider()
    model = nullion_client_as_langchain_chat_model(provider)
    tools = nullion_tools_as_langchain_tools(registry, allowed_tools=['file_write', 'file_read'],
        principal_id='workspace:cedar', cleanup_scope='framework-contract', tool_result_callback=results.append)
    saver = InMemorySaver()
    if engine == 'langchain':
        from langchain.agents import create_agent
        graph = create_agent(model=model, tools=tools, checkpointer=saver)
    else:
        from deepagents import create_deep_agent
        graph = create_deep_agent(model=model, tools=tools, checkpointer=saver)
    config = {'configurable': {'thread_id': f'{engine}-cedar'}, 'recursion_limit': 12}
    started = time.monotonic()
    response = await graph.ainvoke({'messages': [HumanMessage(content=prompt)]}, config=config)
    assert response['messages'][-1].content == answer
    assert path.read_text() == content
    assert provider.calls == 3
    assert [(result.tool_name, result.status) for result in results] == [('file_write', 'completed'), ('file_read', 'completed')]
    saved = await graph.aget_state(config)
    assert saved.values['messages'][-1].content == answer and not saved.next
    assert time.monotonic() - started < 15


@pytest.mark.asyncio
async def test_typed_interrupt_resumes_once_with_checkpoint():
    from typing import TypedDict
    from langgraph.graph import StateGraph, START, END
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import interrupt, Command
    class State(TypedDict):
        selected: str
    observed = []
    def choose(state):
        choice = interrupt({'kind': 'numbered_choice', 'choices': [{'value': 1}, {'value': 2}]})
        observed.append(choice)
        return {'selected': str(choice)}
    builder = StateGraph(State);builder.add_node('choose', choose);builder.add_edge(START, 'choose');builder.add_edge('choose', END)
    graph = builder.compile(checkpointer=InMemorySaver())
    config = {'configurable': {'thread_id': 'typed-choice'}}
    paused = await graph.ainvoke({'selected': ''}, config)
    assert paused['__interrupt__'] and not observed
    completed = await graph.ainvoke(Command(resume=2), config)
    assert completed['selected'] == '2' and observed == [2]
    assert not (await graph.aget_state(config)).next
