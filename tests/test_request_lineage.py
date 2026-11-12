from types import SimpleNamespace
import json
import pytest
from datetime import datetime,UTC,timedelta

from nullion.request_lineage import selected_request_context
from nullion.request_lineage import recent_context_only_turns
from nullion.conversation_runtime import ConversationTurnDisposition as D


def fixture():
    rows=[{'conversation_id':'web:a','workspace_id':'workspace-a','turn_id':'root',
        'parent_turn_id':None,'user_message':'出演者とプロの身長を調べて。','assistant_reply':'対象は？\n1. 出演者\n2. 両方'},
        {'conversation_id':'web:a','workspace_id':'workspace-a','turn_id':'units',
        'parent_turn_id':'root','user_message':'2','assistant_reply':'単位は？\n1. ft\n2. cm'}]
    stored={r['turn_id']:SimpleNamespace(turn_id=r['turn_id'],conversation_id='web:a',branch_id='selected',
        parent_turn_id=r['parent_turn_id'],status='completed',user_message=r['user_message'],
        created_at=datetime.now(UTC)-timedelta(seconds=5),
        disposition=D.INDEPENDENT if r['turn_id']=='root' else D.CONTINUE) for r in rows}
    store=SimpleNamespace(list_recent_conversation_events_after_reset=lambda *a,**kw:rows,
        get_conversation_turn=stored.get,get_conversation_branch=lambda identity:None)
    turn=SimpleNamespace(turn_id='current',branch_id='selected',parent_turn_id='units',
        disposition=D.CONTINUE,conversation_id='web:a')
    return store,turn,rows


def test_original_goal_survives_multiple_identity_selected_clarifications():
    store,turn,rows=fixture()
    result=selected_request_context(store,turn,'workspace-a')
    assert result['original_request']==rows[0]['user_message']
    assert [r['turn_id'] for r in result['turns']]==['root','units']
    assert not result['truncated']


@pytest.mark.parametrize('protection',['workspace','conversation','reset','cycle','cancelled','independent'])
def test_history_does_not_cross_boundaries_or_select_an_unrelated_goal(protection):
    store,turn,rows=fixture()
    if protection=='workspace':rows[1]['workspace_id']='workspace-b'
    elif protection=='conversation':store.get_conversation_turn('units').conversation_id='web:b'
    elif protection=='reset':
        store.list_recent_conversation_events=lambda *a,**kw:[{'created_at':datetime.now(UTC).isoformat()}]
    elif protection=='cycle':store.get_conversation_turn('units').parent_turn_id='units'
    elif protection=='cancelled':store.get_conversation_branch=lambda identity:SimpleNamespace(status='cancelled')
    else:
        turn.disposition=D.INDEPENDENT
        store.list_recent_conversation_events_after_reset=lambda *a,**kw:pytest.fail('Plain turns must not load ancestry')
    result=selected_request_context(store,turn,'workspace-a')
    if protection=='cycle':assert result['truncated'] and result['original_request'] is None
    else:assert result is None


def test_selected_context_is_bounded_and_exposes_truncation():
    store,turn,rows=fixture()
    store.get_conversation_turn('root').user_message='あ'*10000
    rows[0].update(user_message='あ'*10000,assistant_reply='語'*10000)
    result=selected_request_context(store,turn,'workspace-a')
    assert result['original_request']=='あ'*10000
    assert result['truncated'] and len(json.dumps(result,ensure_ascii=False))<26000


def test_inflight_parent_goal_survives_before_any_assistant_reply_is_saved():
    store,turn,rows=fixture();rows.clear()
    result=selected_request_context(store,turn,'workspace-a')
    assert result['original_request']=='出演者とプロの身長を調べて。'
    assert all(row['assistant']=='' for row in result['turns'])


def test_review_keeps_ancestry_even_when_a_formatted_quote_has_no_text_match():
    from nullion.agent_orchestrator import _completion_review_request_context
    store,turn,rows=fixture();lineage=selected_request_context(store,turn,'workspace-a')
    context=_completion_review_request_context({'tool_flow_context':{
        'selected_request_context':lineage,
        'request_context':{'quoted_message':{'reply_to_text':'単位は？','reply_to_from_bot':True}}}})
    assert context['selected_request_context']['original_request']==rows[0]['user_message']
    assert context['request_context']['quoted_message']['reply_to_text']=='単位は？'


@pytest.mark.parametrize('current',['interrupt','continue'])
def test_replacement_goal_keeps_linked_receipt_facts_without_restoring_the_old_goal(current):
    store,turn,rows=fixture()
    root=store.get_conversation_turn('root');root.user_message='Analyze this receipt.'
    rows[0]['assistant_reply']='Food details. '*55+'Receipt 05B33: eleven sauces were requested.'
    replacement=store.get_conversation_turn('units');replacement.user_message='Notify management only.';replacement.disposition=D.INTERRUPT
    rows[1]['assistant_reply']='The page action failed.'
    if current=='interrupt':
        turn.parent_turn_id='root';turn.disposition=D.INTERRUPT;turn.user_message='Notify management only.'
    context=selected_request_context(store,turn,'workspace-a')
    assert context['original_request']=='Notify management only.'
    receipt=next(r for r in context['turns'] if r['turn_id']=='root')
    assert receipt['context_only'] is True
    assert 'eleven sauces' in receipt['assistant']
    assert not context.get('original_request','').startswith('Analyze')


def test_recovery_references_are_post_reset_owned_bounded_and_never_goals():
    store, turn, rows = fixture()
    rows.extend([
        {'conversation_id': 'web:foreign', 'workspace_id': 'workspace-a', 'turn_id': 'foreign'},
        {'conversation_id': 'web:a', 'workspace_id': 'workspace-b', 'turn_id': 'other-workspace'},
        {'conversation_id': 'web:a', 'workspace_id': 'workspace-a', 'turn_id': 'large',
         'user_message': '語' * 5000, 'assistant_reply': '事実' * 5000},
    ])
    context = recent_context_only_turns(store, turn, 'workspace-a', exclude_ids={'units'})
    assert [row['turn_id'] for row in context] == ['root', 'large']
    assert all(row['context_only'] is True for row in context)
    assert sum(len(row['user']) + len(row['assistant']) for row in context) <= 12000
    assert len(context[-1]['user']) == 1000
    assert len(context[-1]['assistant']) == 4000
    store.list_recent_conversation_events_after_reset = lambda *a, **kw: []
    assert recent_context_only_turns(store, turn, 'workspace-a') == []
