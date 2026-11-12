"""Source descriptors follow typed ancestry, never prose or broad history."""
from datetime import UTC,datetime
from types import SimpleNamespace
import pytest
from nullion.chat_operator import _turn_source_attachments
from nullion.conversation_runtime import ConversationTurn,ConversationTurnDisposition,ConversationBranch,ConversationBranchStatus
from nullion.runtime import bootstrap_persistent_runtime


def turn(runtime,identity,parent=None,disposition='independent',branch=None,status='accepted',conversation='web:thread'):
    value=ConversationTurn(identity,conversation,branch or identity,parent,ConversationTurnDisposition(disposition),'記録',status,datetime.now(UTC))
    runtime.store.add_conversation_turn(value)
    runtime.store.add_conversation_branch(ConversationBranch(value.branch_id,conversation,ConversationBranchStatus.ACTIVE,identity))
    return SimpleNamespace(turn=value)


@pytest.mark.parametrize('filename',['source.png','資料.pdf','بيانات.csv'])
def test_inflight_revision_keeps_sources_before_an_assistant_turn_exists(tmp_path,filename):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');path=tmp_path/filename;path.write_bytes(b'source')
    first=turn(runtime,'first');descriptor={'name':filename,'path':str(path),'media_type':'application/octet-stream'}
    _turn_source_attachments(runtime,first,[descriptor])
    first.turn.status='cancelled'
    branch=runtime.store.get_conversation_branch('first');branch.status=ConversationBranchStatus.SUPERSEDED;branch.superseded_by_branch_id='second'
    runtime.store.add_conversation_branch(branch)
    second=turn(runtime,'second','first','revise')
    assert _turn_source_attachments(runtime,second,[])==[descriptor]
    second.turn.status='cancelled';branch=runtime.store.get_conversation_branch('second')
    branch.status=ConversationBranchStatus.SUPERSEDED;branch.superseded_by_branch_id='third';runtime.store.add_conversation_branch(branch)
    third=turn(runtime,'third','second','revise')
    assert _turn_source_attachments(runtime,third,[])==[descriptor]
    runtime.store.set_conversation_head('web:thread',active_branch_id='third',active_turn_id='third')
    runtime.checkpoint()
    restarted=bootstrap_persistent_runtime(tmp_path/'runtime.db')
    assert _turn_source_attachments(restarted,SimpleNamespace(turn=restarted.store.get_conversation_turn('third')),[])==[descriptor]


@pytest.mark.parametrize('boundary',['independent','cancelled','reset','cross_conversation'])
def test_sources_do_not_cross_unselected_or_cancelled_request_boundaries(tmp_path,boundary):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');path=tmp_path/'source.png';path.write_bytes(b'source')
    first=turn(runtime,'first');_turn_source_attachments(runtime,first,[{'name':path.name,'path':str(path)}])
    if boundary=='cancelled':
        branch=runtime.store.get_conversation_branch('first');branch.status=ConversationBranchStatus.CANCELLED;runtime.store.add_conversation_branch(branch)
    if boundary=='reset':runtime.store.add_conversation_event({'conversation_id':'web:thread','event_type':'conversation.session_reset','created_at':datetime.now(UTC).isoformat()})
    second=turn(runtime,'second','first','independent' if boundary=='independent' else 'continue',
        conversation='web:other' if boundary=='cross_conversation' else 'web:thread')
    assert _turn_source_attachments(runtime,second,[])==[]


def test_new_and_inherited_sources_are_deduped_and_missing_source_is_explicit(tmp_path):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');path=tmp_path/'source.png';path.write_bytes(b'source')
    descriptor={'name':path.name,'path':str(path)};first=turn(runtime,'first')
    _turn_source_attachments(runtime,first,[descriptor]);second=turn(runtime,'second','first','continue')
    assert _turn_source_attachments(runtime,second,[descriptor])==[descriptor]
    path.unlink();third=turn(runtime,'third','second','continue')
    assert _turn_source_attachments(runtime,third,[])==[{**descriptor,'status':'unavailable'}]


def test_plain_turn_never_reads_source_history_or_adds_a_model_call(tmp_path,monkeypatch):
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');current=turn(runtime,'plain')
    def forbidden(*a,**k):raise AssertionError('Plain turn loaded attachment history')
    monkeypatch.setattr(runtime.store.__class__,'list_recent_conversation_events_after_reset',forbidden)
    assert _turn_source_attachments(runtime,current,[])==[]


def test_request_text_and_media_share_revised_branch_lineage(tmp_path):
    from nullion.chat_operator import _linked_request_user_messages
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');first=turn(runtime,'first')
    first.turn.user_message='この写真の出演者について調べてください';first.turn.status='cancelled'
    branch=runtime.store.get_conversation_branch('first');branch.status=ConversationBranchStatus.SUPERSEDED
    branch.superseded_by_branch_id='second';runtime.store.add_conversation_branch(branch)
    second=turn(runtime,'second','first','revise')
    assert _linked_request_user_messages(runtime,second)==(first.turn.user_message,)
    branch.status=ConversationBranchStatus.CANCELLED;runtime.store.add_conversation_branch(branch)
    assert _linked_request_user_messages(runtime,second)==()
    independent=turn(runtime,'other','first','independent')
    assert _linked_request_user_messages(runtime,independent)==()


def test_selected_request_text_does_not_cross_session_reset(tmp_path):
    from nullion.chat_operator import _linked_request_user_messages
    runtime=bootstrap_persistent_runtime(tmp_path/'runtime.db');first=turn(runtime,'first')
    runtime.store.add_conversation_event({'conversation_id':'web:thread','event_type':'conversation.session_reset',
        'created_at':datetime.now(UTC).isoformat()})
    second=turn(runtime,'second','first','continue')
    assert _linked_request_user_messages(runtime,second)==()
