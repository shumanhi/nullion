"""Recovery retains caller-authorized context; it does not reload global user data."""
from types import SimpleNamespace

from nullion.tools import ToolRegistry, ToolResult


def test_backend_preserves_explicit_profile_history_without_mutating_it():
    from nullion.chat_backend import generate_chat_reply
    history = [{'role': 'system', 'content': [{'type': 'text', 'text': 'Address: Example City'}]},
               {'role': 'user', 'content': [{'type': 'text', 'text': '以前の条件'}]}]
    calls = []
    class Agent:
        def run_turn(self, **kwargs):
            calls.append(kwargs)
            kwargs['conversation_history'].append({'role': 'assistant', 'content': []})
            return SimpleNamespace(final_text='done')
    assert generate_chat_reply(message='続けて', conversation_history=history,
        agent_orchestrator=Agent(), active_tool_registry=ToolRegistry()) == 'done'
    assert len(history) == 2
    assert calls[0]['conversation_history'][:2] == history


def test_backend_adapter_forwards_authorized_history_and_records_receipts(monkeypatch):
    from nullion import chat_operator
    receipt = ToolResult('read-1', 'lookup_source', 'completed', {'price': 8.10})
    captured = []
    def generate(*, message, conversation_history, live_tool_result_recorder):
        captured.append(conversation_history)
        live_tool_result_recorder(receipt)
        return 'done'
    monkeypatch.setattr(chat_operator, 'generate_chat_reply', generate)
    history = [{'role': 'system', 'content': [{'type': 'text', 'text': '住所: Example City'}]}]
    results = []
    runtime = SimpleNamespace(active_tool_registry=ToolRegistry())
    assert chat_operator._generate_backend_reply(runtime, prompt='続けて',
        conversation_context=None, memory_context=None, conversation_history=history,
        live_tool_results=results) == 'done'
    assert captured == [history]
    assert results == [receipt]


def test_backend_reuses_scoped_registry_without_widening_authority():
    from nullion.chat_backend import generate_chat_reply
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence
    scoped = ScopedTurnToolRegistry(ToolRegistry(), evidence=TurnToolEvidence())
    calls = []
    class Agent:
        def run_turn(self, **kwargs):
            calls.append(kwargs['tool_registry'])
            return SimpleNamespace(final_text='done')
    assert generate_chat_reply(message='続けて', active_tool_registry=scoped, agent_orchestrator=Agent()) == 'done'
    assert calls == [scoped]
    assert not scoped.can_invoke_tool('email_send')
