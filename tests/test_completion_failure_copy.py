"""Internal recovery checklists must not become the terminal answer."""
import pytest


@pytest.mark.parametrize('disposition', ['retry', 'blocked'])
@pytest.mark.parametrize('requirement', [
    'Replace the draft with a concise delivery message; exclude the duplicate working file.',
    'Reemplaza el borrador y excluye los archivos de trabajo.',
    '内部の作業ファイルを除き、回答を書き直してください。',
])
def test_terminal_failure_never_copies_internal_recovery_requirements(disposition, requirement):
    from nullion.agent_orchestrator import _CompletionReviewDecision, _completion_review_open_task_reply
    decision = _CompletionReviewDecision(disposition, (requirement,))
    reply = _completion_review_open_task_reply(decision)
    assert requirement not in reply
    assert reply == "I couldn't complete this request. The final result is incomplete."


def test_freeform_reviewer_failure_is_not_publication_authority():
    from nullion.agent_orchestrator import _CompletionReviewDecision, _completion_review_open_task_reply
    explanation = 'The source rejected the request. Your account needs to be reconnected in Settings.'
    assert _completion_review_open_task_reply(_CompletionReviewDecision('blocked', ('internal recovery',),
        user_facing_failure=explanation)) == "I couldn't complete this request. The final result is incomplete."
