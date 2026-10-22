from types import SimpleNamespace

import pytest

from nullion import chat_operator
from nullion.tools import ToolRegistry, ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision


ANSWERS = [
    'You need to review which services can access your email. Connected apps can process message content.',
    'Revisa los permisos: las aplicaciones conectadas pueden procesar el contenido del correo.',
    '接続するアプリの権限と、メールの内容がどのように処理されるかを確認してください。',
    '1. Review which services can access your email.\n2. Connected apps can process message content.',
    '1. 接続するアプリの権限を確認してください。\n2. メールの内容が処理される場合があります。',
]


def terminal_registry():
    registry = ToolRegistry()
    registry.register(ToolSpec(name='terminal_exec', description='Core shell execution',
        risk_level=ToolRiskLevel.HIGH, side_effect_class=ToolSideEffectClass.DANGEROUS_EXEC,
        requires_approval=True, timeout_seconds=10), lambda invocation: None)
    return registry


def result(text, tools=()):
    return SimpleNamespace(final_text=text, tool_results=list(tools), artifacts=[], suspended_for_approval=False)


@pytest.mark.parametrize('answer', ANSWERS)
def test_terminal_evidence_cannot_replace_answer(answer):
    probe = ToolResult('probe', 'terminal_exec', 'completed', {
        'exit_code': 0, 'stdout': 'Local terminal capability verified; no local files or system state are needed.\n'})
    assert chat_operator._sanitize_chat_reply(prompt='接続時のプライバシー上の懸念は？', reply=answer, tool_results=[probe]) == answer


@pytest.mark.parametrize('answer', ANSWERS)
def test_plain_answer_does_not_invoke_recovery_classifier_or_shell(monkeypatch, answer):
    registry = terminal_registry()
    original = result(answer)
    def forbidden(**kwargs):
        raise AssertionError('No recovery model or shell is needed for an ordinary answer')
    monkeypatch.setattr(chat_operator, 'build_turn_tool_scope_decision', forbidden)
    common = chat_operator._run_chat_turn_common_retries(initial_result=original, base_history=[],
        active_tool_registry=registry, base_tool_registry=registry, model_client=object(),
        user_message='接続時のプライバシー上の懸念は？', evidence=TurnToolEvidence(),
        run_turn=forbidden, include_local_system_fallback=True)
    assert common.result is original
    actual, _, retried = chat_operator._run_chat_turn_local_system_fallback_retry(
        initial_result=original, base_history=[], active_tool_registry=registry,
        model_client=object(), user_message='接続時のプライバシー上の懸念は？',
        evidence=TurnToolEvidence(), run_turn=forbidden)
    assert actual is original and not retried


def test_structured_shell_scope_still_recovers_without_another_classifier(monkeypatch):
    registry = terminal_registry()
    scoped = ScopedTurnToolRegistry(registry, evidence=TurnToolEvidence(), tool_scope_decision=TurnToolScopeDecision(
        valid=True, requested_tool_names=('terminal_exec',), required_tool_names=('terminal_exec',)))
    monkeypatch.setattr(chat_operator, 'build_turn_tool_scope_decision',
        lambda **kw: pytest.fail('Reuse validated scope'))
    approval = SimpleNamespace(final_text='承認が必要です。', tool_results=[], artifacts=[], suspended_for_approval=True)
    def retry(history, visible):
        assert visible.get_spec('terminal_exec').requires_approval
        return approval
    actual, _, retried = chat_operator._run_chat_turn_local_system_fallback_retry(initial_result=result('実行待ちです。'),
        base_history=[], active_tool_registry=scoped, base_tool_registry=registry, model_client=object(),
        user_message='実行してください。', evidence=TurnToolEvidence(), run_turn=retry)
    assert actual is approval and retried


@pytest.mark.parametrize('valid', [False, True])
def test_scope_without_shell_never_forces_shell_from_draft_words(monkeypatch, valid):
    registry = terminal_registry()
    original = result('<tool_request tool="request_tool_scope" capabilities="local_shell"></tool_request>')
    monkeypatch.setattr(chat_operator, 'build_turn_tool_scope_decision', lambda **kw: TurnToolScopeDecision(valid=valid))
    actual, _, retried = chat_operator._run_chat_turn_local_system_fallback_retry(initial_result=original,
        base_history=[], active_tool_registry=registry, model_client=object(), user_message='対象の状態は？',
        evidence=TurnToolEvidence(), run_turn=lambda *args: pytest.fail('Scope did not authorize a shell retry'))
    assert actual is original and not retried


def test_empty_answer_can_still_fall_back_to_completed_command_receipt():
    output = ToolResult('shell', 'terminal_exec', 'completed', {'exit_code': 0, 'stdout': '12'})
    assert '12' in chat_operator._sanitize_chat_reply(prompt='実行してください。', reply='', tool_results=[output])
