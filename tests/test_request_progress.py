import asyncio
import time

import pytest


@pytest.mark.parametrize('output,waiting', [
    ({'requires_approval': True, 'approval_id': 'native-37'}, True),
    ({'requires_approval': False, 'approval_id': 'native-37'}, False),
    ({'requires_approval': 'true', 'approval_id': 'native-37'}, False),
    ({'requires_approval': True}, False),
    ({'requires_approval': True, 'approval_id': ' '}, False),
    ({'requires_approval': True, 'approval_id': 37}, False),
])
def test_native_denied_receipt_only_waits_for_a_typed_pending_approval(output, waiting):
    from nullion.request_progress import RequestProgress, tool_operation_progress
    from nullion.tools import ToolSpec, ToolResult, ToolRiskLevel, ToolSideEffectClass
    spec = ToolSpec('不透明', 'PRIVATE /path', ToolRiskLevel.HIGH, ToolSideEffectClass.READ, True, 2)
    result = ToolResult('native', '不透明', 'denied', {**output, 'message': 'PRIVATE result'})
    event = tool_operation_progress(result, spec)
    assert (event['operation_progress']['status'] == 'approval_required') is waiting
    events = []
    RequestProgress(events.append).emit(event)
    assert ('Waiting for your approval' in events[-1]['text']) is waiting
    assert 'PRIVATE' not in str(events) and 'native-37' not in str(events)


def test_same_page_count_shows_changes_between_reading_review_and_evidence_repair():
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    for phase in ('reading_pages','reviewing_captured_evidence','verifying_evidence','finishing_source_review'):
        progress.emit({'operation_id':'worker','activity_phase':phase,
            'observed_progress':{'pages_checked':9,'findings_found':37}})
    texts=[e['text'] for e in events if e.get('status_kind')=='request_progress']
    assert len(texts)==4
    assert texts[1].startswith('Reviewing captured page details')
    assert texts[2].startswith('Verifying source details')
    assert texts[-1].startswith('Finishing source review')
    assert all('9 pages checked' in text for text in texts)


@pytest.mark.parametrize('phase',['SECRET /private/page',{'text':'SECRET'},['SECRET'],None])
def test_source_activity_only_accepts_fixed_safe_phase_values(phase):
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_id':'worker','activity_phase':phase,
        'observed_progress':{'pages_checked':9,'findings_found':37}})
    assert events[-1]['text']=='Checking sources · 9 pages checked'


@pytest.mark.parametrize('tags,label',[(('calendar','account_read'),'Calendar checked'),
    (('email','account_read'),'Mail checked'),(('scheduler_inventory',),'Scheduled tasks checked')])
def test_completed_native_metadata_shows_safe_activity_alongside_source_counts(tags,label):
    from nullion.request_progress import RequestProgress,tool_operation_progress
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    spec=ToolSpec('不透明','SECRET /private/path',ToolRiskLevel.LOW,ToolSideEffectClass.READ,
        False,2,capability_tags=tags)
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_id':'worker','activity_phase':'reviewing_findings',
        'observed_progress':{'pages_checked':9,'findings_found':37}})
    for status in ('running','failed','completed'):
        progress.emit(tool_operation_progress(ToolResult('native','不透明',status,
            {'results':['SECRET /private/path']}),spec))
        assert (label in events[-1]['text']) == (status=='completed')
    assert '9 pages checked' in events[-1]['text'] and label in events[-1]['text']
    assert 'SECRET' not in str(events) and '不透明' not in str(events)


def test_source_phase_changes_preserve_counts_and_retire_calendar_receipt():
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_progress':{'id':'calendar','status':'completed','category':'calendar'}})
    progress.emit({'operation_id':'search','source_progress':{
        'unit':'searches','completed':2,'failed':0,'total':2,'records':16}})
    progress.emit({'operation_id':'worker','activity_phase':'verifying_evidence',
        'observed_progress':{'pages_checked':9,'findings_found':37}})
    assert events[-1]['text']=='Verifying source details · 2 searches · 16 results · 9 pages checked'


@pytest.mark.parametrize('receipt,label',[
    ({'category':'calendar'},'Calendar checked'),
    ({'category':'mail'},'Mail checked'),
    ({'category':'scheduled_tasks'},'Scheduled tasks checked'),
    ({'produces_file':True},'File prepared'),
])
@pytest.mark.parametrize('next_event',[
    {'operation_progress':{'id':'page','status':'running','activity_kind':'browser_navigate'}},
    {'operation_id':'page','observed_progress':{'pages_checked':1,'findings_found':1}},
    {'id':'phase-prepare-artifacts','status':'running'},
])
def test_completion_receipts_expire_when_another_activity_starts(receipt,label,next_event):
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_progress':{'id':'receipt','status':'completed',**receipt}})
    assert label in events[-1]['text']
    progress.emit(next_event)
    assert label not in events[-1]['text']


def test_late_calendar_receipt_does_not_override_an_active_file_producer():
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_progress':{'id':'calendar','status':'running','category':'calendar'}})
    progress.emit({'operation_progress':{'id':'pdf','status':'running','produces_file':True}})
    progress.emit({'operation_progress':{'id':'calendar','status':'completed','category':'calendar'}})
    assert events[-1]['text'].startswith('Preparing the requested file')
    assert 'Calendar checked' not in events[-1]['text']


@pytest.mark.parametrize('tool,running,completed',[
    ('web_search','Searching the web','Reviewing search results'),
    ('browser_navigate','Opening a page','Reviewing page details'),
    ('browser_read_pages','Reading pages','Reviewing page details'),
    ('browser_run_task','Checking the browser','Reviewing findings'),
    ('file_read','Reading a file','Reviewing file contents'),
])
def test_native_tool_starts_and_completions_have_safe_current_activity(tool,running,completed):
    from nullion.request_progress import RequestProgress,tool_operation_progress
    from nullion.tools import ToolSpec,ToolResult,ToolRiskLevel,ToolSideEffectClass
    spec=ToolSpec(tool,'SECRET /private/source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,2)
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_id':'source','observed_progress':{'pages_checked':9,'findings_found':37}})
    for status,label in [('running',running),('completed',completed)]:
        progress.emit(tool_operation_progress(ToolResult('native',tool,status,{'text':'SECRET'}),spec))
        assert events[-1]['text'].startswith(label) and '9 pages checked' in events[-1]['text']
    assert 'SECRET' not in str(events)


def test_completed_source_keeps_review_phase_while_primary_agent_prepares_response():
    from nullion.request_progress import RequestProgress,OPERATION_KIND
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_id':'source','activity_phase':'finishing_source_review',
        'observed_progress':{'pages_checked':9,'findings_found':37}})
    progress.emit({'status_kind':OPERATION_KIND,'operation_progress':{'id':'source','status':'completed'}})
    assert events[-1]['text']=='Reviewing findings · 9 pages checked'


@pytest.mark.parametrize('has_file', [False, True])
def test_terminal_preparation_label_is_generic_for_text_and_attachments(monkeypatch, has_file):
    from nullion.request_progress import RequestProgress
    from nullion import preferences
    monkeypatch.setattr(preferences, 'load_preferences', lambda: {'emoji_level':'none', 'markdown_style':'plain'})
    events = []
    progress = RequestProgress(events.append)
    if has_file:
        progress.emit({'operation_progress':{'id':'producer', 'status':'completed', 'produces_file':True}})
    progress.emit({'id':'phase-prepare-artifacts', 'status':'running'})
    assert events[-1]['status_kind'] == 'request_progress'
    assert 'Preparing your response' in events[-1]['text']
    assert 'files for delivery' not in events[-1]['text']


def test_acceptance_starts_shared_notice_before_context_or_model_work(monkeypatch):
    from nullion.request_progress import RequestProgress
    from nullion import preferences
    monkeypatch.setattr(preferences, 'load_preferences', lambda: {'emoji_level':'none','markdown_style':'plain'})
    events=[];progress=RequestProgress(events.append)
    progress.emit({'id':'prepare','status':'done'})
    assert events[-1]['status_kind']=='request_progress'
    from nullion.chat_text import WORKING_ACK_SUMMARY
    assert events[-1]['text']==WORKING_ACK_SUMMARY.removesuffix('.')


def test_telegram_reply_lookup_never_copies_full_conversation_history(monkeypatch):
    from nullion.runtime_store import RuntimeStore
    from nullion.telegram_app import _latest_chat_turn_for_reply
    from types import SimpleNamespace
    store=RuntimeStore()
    for index in range(80):
        store.add_conversation_event({'conversation_id':'telegram:77','event_type':'conversation.chat_turn',
            'turn_id':str(index),'assistant_reply':'Old result '+str(index)})
    store.add_conversation_event({'conversation_id':'telegram:77','event_type':'conversation.chat_turn',
        'turn_id':'current','assistant_reply':'Current result'})
    def forbidden(*args,**kwargs):raise AssertionError('Full history copied in the delivery hot path')
    monkeypatch.setattr(RuntimeStore,'list_conversation_events',forbidden)
    result=_latest_chat_turn_for_reply(SimpleNamespace(store=store),chat_id='77',reply='Current result')
    assert result['turn_id']=='current'
    assert _latest_chat_turn_for_reply(SimpleNamespace(store=store),chat_id='77',reply='Unknown') is None


def test_progress_uses_typed_evidence_preserves_failures_and_ignores_private_prose(monkeypatch):
    from nullion.request_progress import RequestProgress
    from nullion import preferences
    monkeypatch.setattr(preferences, 'load_preferences', lambda: {'emoji_level':'none','markdown_style':'plain'})
    events = []
    progress = RequestProgress(events.append)
    progress.emit({'id':'phase-start-model','status':'running','detail':'SECRET path /private'})
    assert events[-1]['text'] == 'Planning the next step'
    progress.emit({'operation_id':'a','source_progress':{'unit':'pages','completed':1,'failed':1,'total':3,'records':0}})
    visible = [e['text'] for e in events if e.get('status_kind')=='request_progress']
    assert '1/3 pages read' in visible[-1]
    assert progress.batches['a']['failed']==1
    assert not any('SECRET' in s or '/private' in s or '%' in s for s in visible)
    before = len(visible)
    progress.emit({'operation_id':'b','source_progress':{'unit':'pages','completed':True,'failed':0,'total':1,'records':0}})
    assert len([e for e in events if e.get('status_kind')=='request_progress']) == before
    progress.emit({'operation_id':'a','source_progress':{'unit':'pages','completed':2,'failed':1,'total':3,'records':0}})
    assert '2 pages read' in events[-1]['text']
    assert events[-1]['completion_scope']=='operation'


def test_parallel_tool_callbacks_share_one_consistent_request_snapshot():
    from concurrent.futures import ThreadPoolExecutor
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    def emit(index):
        progress.emit({'operation_id':str(index),'source_progress':{
            'unit':'pages','completed':1,'failed':0,'total':1,'records':0}})
    with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(emit,range(20)))
    assert '20 pages read' in events[-1]['text']


def test_adaptive_progress_counts_use_real_observations_without_inventing_a_total():
    from nullion.request_progress import RequestProgress
    events = []
    progress = RequestProgress(events.append)
    for pages, findings in ((1,0), (2,3), (2,3)):
        progress.emit({'operation_id':'owned', 'observed_progress':{
            'pages_checked':pages, 'findings_found':findings}, 'detail':'PRIVATE SOURCE TEXT'})
    visible = [e['text'] for e in events if e.get('status_kind')=='request_progress']
    assert len(visible) == 2 and '2 pages checked' in visible[-1]
    assert all('PRIVATE' not in text and '%' not in text and ' of ' not in text for text in visible)


def test_actual_answer_output_ends_progress_without_republishing_status():
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    progress.emit({'id':'phase-start-model','status':'running'})
    progress.emit({'status_kind':'request_progress','status':'done','text':''})
    progress.emit({'id':'phase-save-conversation','status':'running'})
    statuses=[e for e in events if e.get('status_kind')=='request_progress']
    assert statuses[-1]['status']=='done' and len(statuses)==2


def test_preparation_progress_uses_typed_phases_without_disclosing_context():
    from nullion.request_progress import RequestProgress
    events=[];progress=RequestProgress(events.append)
    for phase,text in [('phase-check-task-state','Checking task context'),
            ('phase-load-memory','Loading saved context'),('phase-select-tools','Preparing the tools for your request'),
            ('phase-build-context','Preparing request context'),('phase-start-model','Planning the next step')]:
        progress.emit({'id':phase,'status':'running','detail':'PRIVATE account and saved memory'})
        assert events[-1]['text']==text and events[-1]['phase_id']==phase
    assert all('PRIVATE' not in e.get('text','') for e in events)


@pytest.mark.asyncio
async def test_progress_delivery_timings_follow_acknowledged_coalesced_views_and_isolate_observer_failures():
    from nullion.request_progress import RequestProgressDelivery
    import asyncio
    acknowledgements=[];timings=[];sent=asyncio.Event();edited=asyncio.Event()
    async def send(text):
        acknowledgements.append(('send',text));sent.set();return 'receipt'
    async def edit(receipt,text):
        acknowledgements.append(('edit',text));edited.set();return True
    async def remove(receipt):pass
    def observe(fact):
        assert len(acknowledgements)>len(timings)
        timings.append(fact)
        raise RuntimeError('Optional observer failure')
    delivery=RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,remove=remove,
        delay=.02,interval=.01,on_published=observe)
    delivery.emit({'status_kind':'request_progress','phase_id':'prepare','text':'On it'})
    delivery.emit({'status_kind':'request_progress','phase_id':'phase-load-memory','text':'Loading saved context'})
    await asyncio.wait_for(sent.wait(),.3)
    delivery.emit({'status_kind':'request_progress','phase_id':'operation','text':'Reading a page'})
    await asyncio.wait_for(edited.wait(),.3)
    await delivery.finish()
    assert acknowledgements==[('send','On it'),('edit','Reading a page')]
    assert timings==[{'operation':'send','phase_id':'prepare'},{'operation':'edit','phase_id':'operation'}]


@pytest.mark.asyncio
async def test_activity_arriving_during_acceptance_send_supersedes_older_preparation():
    from nullion.request_progress import RequestProgressDelivery
    import asyncio
    entered=asyncio.Event();release=asyncio.Event();edited=asyncio.Event();calls=[]
    async def send(text):
        calls.append(('send',text));entered.set();await release.wait();return 'receipt'
    async def edit(receipt,text):
        calls.append(('edit',text));edited.set();return True
    async def remove(receipt):pass
    delivery=RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,remove=remove,
        delay=.01,interval=.01)
    delivery.emit({'status_kind':'request_progress','phase_id':'prepare','text':'On it'})
    delivery.emit({'status_kind':'request_progress','phase_id':'phase-load-memory','text':'Loading saved context'})
    await asyncio.wait_for(entered.wait(),.3)
    delivery.emit({'status_kind':'request_progress','phase_id':'operation','text':'Searching the web'})
    release.set()
    await asyncio.wait_for(edited.wait(),.3)
    await delivery.finish()
    assert calls==[('send','On it'),('edit','Searching the web')]


@pytest.mark.parametrize('status', ['running', 'completed', 'failed', 'denied', 'approval_required'])
def test_private_tool_lifecycle_has_safe_shared_progress_without_exposing_results(status):
    from nullion.request_progress import RequestProgress, tool_operation_progress
    from nullion.conversation_history_tools import CHAT_HISTORY_SEARCH_TOOL_SPEC
    from nullion.tools import ToolResult
    events = []
    progress = RequestProgress(events.append)
    event = tool_operation_progress(ToolResult('owned', 'opaque', status, {
        'suppress_activity': True, 'message': 'SECRET /private/path', 'matches': ['private@example.test'],
    }), CHAT_HISTORY_SEARCH_TOOL_SPEC)
    assert event is not None
    progress.emit(event)
    assert events and all(e.get('status_kind') == 'request_progress' for e in events)
    visible = str(events)
    assert all(value not in visible for value in ('SECRET', '/private/path', 'private@example.test', 'opaque'))
    assert ('1 step completed' in visible) == (status == 'completed')
    if status == 'running':
        assert 'Searching saved information' in visible
    if status == 'approval_required':
        assert 'Waiting for your approval' in visible
    if status == 'denied':
        assert 'Searching saved information' not in visible


def test_typed_history_selection_filters_before_copying_and_returns_isolated_records():
    from nullion.runtime_store import RuntimeStore
    class PrivatePayload:
        def __deepcopy__(self, memo):
            raise AssertionError('Unrelated runtime payload must not enter history search')
    store = RuntimeStore(conversation_events=[
        {'event_type': 'conversation.tool_timing', 'conversation_id': 'web:a', 'payload': PrivatePayload()},
        {'event_type': 'conversation.chat_turn', 'conversation_id': 'web:a', 'user_message': '資料', 'assistant_reply': '根拠'},
        {'event_type': 'conversation.session_reset', 'conversation_id': 'web:a'},
    ])
    rows = store.list_conversation_events(event_types=('conversation.chat_turn', 'conversation.session_reset'))
    assert len(rows) == 2
    rows[0]['assistant_reply'] = 'changed'
    assert store.conversation_events[1]['assistant_reply'] == '根拠'
    assert store.list_conversation_events(event_types=()) == []


@pytest.mark.asyncio
async def test_progress_is_delayed_coalesced_removed_and_does_not_block_final_on_slow_transport():
    from nullion.request_progress import RequestProgressDelivery
    calls = []; block = asyncio.Event()
    async def send(text): calls.append(('send',text)); return 'receipt'
    async def edit(handle,text): calls.append(('edit',text)); return True
    async def remove(handle):calls.append(('delete',handle))
    def event(text): return {'status_kind':'request_progress','text':text}
    fast = RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,remove=remove,delay=.1)
    fast.emit(event('Working'))
    await fast.finish()
    assert not calls
    delivery = RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,remove=remove,delay=.01,interval=.02)
    delivery.emit(event('Read 1 page'))
    await asyncio.sleep(.03)
    for i in range(100): delivery.emit(event(f'Read {i} pages'))
    await asyncio.sleep(.06)
    await delivery.finish()
    assert len([c for c in calls if c[0]=='send'])==1
    assert len(calls)<=4 and ('edit','Read 99 pages') in calls and calls[-1][0]=='delete'
    async def slow_send(text):await block.wait();return 'late'
    slow = RequestProgressDelivery(loop=asyncio.get_running_loop(),send=slow_send,edit=edit,remove=remove,delay=0)
    slow.emit(event('Working'))
    await asyncio.sleep(.01)
    start=time.monotonic();await slow.finish()
    assert time.monotonic()-start<.2
    block.set();await slow.cleanup
    assert calls[-1]==('delete','late')


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',['exception','rejected'])
async def test_progress_skips_unchanged_edits_and_recovers_the_owned_message_after_edit_failure(failure):
    from nullion.request_progress import RequestProgressDelivery
    calls=[]
    async def send(text):calls.append(('send',text));return 'receipt'
    async def edit(handle,text):
        assert handle=='receipt'
        calls.append(('edit',text))
        if text=='Reading pages':
            if failure=='exception':raise OSError('Temporary edit failure')
            return False
        return True
    async def remove(handle):calls.append(('delete',handle))
    delivery=RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,
        remove=remove,delay=0,interval=.01)
    for text in ('Opening a page','Opening a page','Reading pages','Preparing the requested file'):
        delivery.emit({'status_kind':'request_progress','text':text})
        await asyncio.sleep(.03)
    await delivery.finish()
    assert calls==[('send','Opening a page'),('edit','Reading pages'),
        ('edit','Preparing the requested file'),('delete','receipt')]


@pytest.mark.asyncio
async def test_queued_progress_stops_at_publication_revocation_and_preserves_the_other_request():
    from nullion.request_progress import RequestProgressDelivery
    allowed = {'a': True, 'b': True}
    calls = []
    def delivery(owner):
        async def send(text): calls.append((owner,'send',text)); return owner
        async def edit(handle,text): calls.append((owner,'edit',text)); return True
        async def remove(handle): calls.append((owner,'remove',handle))
        return RequestProgressDelivery(loop=asyncio.get_running_loop(), send=send, edit=edit,
            remove=remove, can_publish=lambda:allowed[owner], delay=0, interval=.08)
    a, b = delivery('a'), delivery('b')
    a.emit({'status_kind':'request_progress','text':'Started A'})
    b.emit({'status_kind':'request_progress','text':'Started B'})
    await asyncio.sleep(.03)
    a.emit({'status_kind':'request_progress','text':'Queued A'})
    b.emit({'status_kind':'request_progress','text':'Updated B'})
    allowed['a'] = False
    await asyncio.sleep(.12)
    assert ('a','edit','Queued A') not in calls
    assert ('b','edit','Updated B') in calls
    await a.finish()
    await b.finish()
    assert ('a','remove','a') in calls and ('b','remove','b') in calls


@pytest.mark.asyncio
async def test_typing_recovers_after_transient_failure_and_stops_on_cancel(monkeypatch):
    from nullion import telegram_app
    calls=[];success=asyncio.Event()
    async def action(message, **kwargs):
        calls.append('typing')
        if len(calls)==1:raise OSError('Transient network interruption')
        success.set()
    monkeypatch.setattr(telegram_app, '_send_typing_indicator', action)
    monkeypatch.setattr(telegram_app, '_TYPING_KEEPALIVE_INTERVAL_SECONDS', .01)
    task=asyncio.create_task(telegram_app._run_typing_keepalive(object(),runtime=None,text='確認'))
    await asyncio.wait_for(success.wait(),.2)
    await telegram_app._stop_typing_keepalive(task)
    count=len(calls);await asyncio.sleep(.03)
    assert count>=2 and len(calls)==count


@pytest.mark.asyncio
async def test_background_typing_reuses_one_bot_connection_pool(monkeypatch):
    from nullion import telegram_app
    entered=[];exited=[];actions=[];ready=asyncio.Event()
    class Bot:
        async def __aenter__(self):entered.append(self);return self
        async def __aexit__(self,*args):exited.append(self)
        async def send_chat_action(self,**kwargs):
            actions.append(kwargs)
            if len(actions)>=3:ready.set()
    monkeypatch.setattr(telegram_app,'build_telegram_bot',lambda *args:Bot())
    monkeypatch.setattr(telegram_app,'_TYPING_KEEPALIVE_INTERVAL_SECONDS',.01)
    task=asyncio.create_task(telegram_app._run_telegram_chat_typing_keepalive('fixture',chat_id='77',runtime=None,text='確認'))
    await asyncio.wait_for(ready.wait(),.3)
    await telegram_app._stop_typing_keepalive(task)
    assert len(entered)==1 and exited==entered and len(actions)>=3


@pytest.mark.asyncio
@pytest.mark.parametrize('revoked', [False, True])
async def test_optional_progress_failure_never_retries_a_send_or_controls_final_delivery(revoked):
    from nullion.request_progress import RequestProgressDelivery
    from nullion.turn_dispatch_graph import TurnPublicationRevoked
    calls=[]
    async def send(text):
        calls.append(text)
        if revoked:raise TurnPublicationRevoked()
        raise OSError('Status transport unavailable')
    async def unused(*args):raise AssertionError('No progress receipt exists')
    delivery=RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=unused,remove=unused,delay=0)
    delivery.emit({'status_kind':'request_progress','text':'Read 1 page.'})
    await asyncio.sleep(.02)
    delivery.emit({'status_kind':'request_progress','text':'Read 2 pages.'})
    await delivery.finish()
    assert calls==['Read 1 page.']


@pytest.mark.asyncio
async def test_progress_notice_remains_during_upload_without_allowing_new_updates():
    from nullion.request_progress import RequestProgressDelivery
    calls=[]
    async def send(text):calls.append(('send',text));return 'receipt'
    async def edit(*args):raise AssertionError('A frozen notice cannot change')
    async def remove(handle):calls.append(('delete',handle))
    delivery=RequestProgressDelivery(loop=asyncio.get_running_loop(),send=send,edit=edit,remove=remove,delay=0)
    delivery.emit({'status_kind':'request_progress','text':'Preparing files for delivery.'})
    await asyncio.sleep(.01)
    await delivery.freeze()
    delivery.emit({'status_kind':'request_progress','text':'Obsolete update'})
    assert calls==[('send','Preparing files for delivery.')]
    await delivery.finish()
    assert calls[-1]==('delete','receipt')


def test_completed_tools_report_facts_without_repeating_working_results():
    from nullion.request_progress import RequestProgress, OPERATION_KIND
    events=[];progress=RequestProgress(events.append)
    for index in range(4):
        for status in ('running','completed'):
            progress.emit({'status_kind':OPERATION_KIND,'operation_progress':{'id':str(index),'status':status}})
    text=[e['text'] for e in events if e.get('status_kind')=='request_progress']
    assert text[-1]=='Reviewing step results · 4 steps completed'
    assert all('Working with' not in value for value in text)


def test_completed_parallel_read_does_not_hide_running_file_producer():
    from nullion.request_progress import RequestProgress, OPERATION_KIND
    events=[];progress=RequestProgress(events.append)
    for identity,status,file in [('producer','running',True),('read','running',False),('read','completed',False)]:
        progress.emit({'status_kind':OPERATION_KIND,'operation_progress':{
            'id':identity,'status':status,'produces_file':file}})
    assert 'Preparing the requested file' in events[-1]['text']
    assert '1 step completed' in events[-1]['text']


def test_busy_progress_stays_one_compact_line_without_zero_or_internal_counters(monkeypatch):
    from nullion.request_progress import RequestProgress
    from nullion import preferences
    monkeypatch.setattr(preferences,'load_preferences',lambda:{'emoji_level':'none','markdown_style':'plain'})
    events=[];progress=RequestProgress(events.append)
    progress.emit({'operation_id':'search','source_progress':{'unit':'searches','completed':2,'failed':0,'total':2,'records':16}})
    progress.emit({'operation_id':'pages','source_progress':{'unit':'pages','completed':1,'failed':0,'total':1,'records':0}})
    progress.emit({'operation_id':'adaptive','observed_progress':{'pages_checked':3,'findings_found':0}})
    progress.emit({'operation_progress':{'id':'failed','status':'failed'}})
    text=events[-1]['text']
    assert text=='Checking the next step · 2 searches · 16 results · 3 pages checked'
    assert len(text)<80 and '\n' not in text
    assert progress.observations['adaptive']['findings_found']==0
    assert progress.operations['failed']=='failed'


def test_model_round_after_an_opaque_step_keeps_current_activity_visible():
    from nullion.request_progress import RequestProgress, OPERATION_KIND
    events=[]; progress=RequestProgress(events.append)
    progress.emit({'status_kind': OPERATION_KIND, 'operation_progress': {'id':'opaque','status':'completed'}})
    assert events[-1]['text'].startswith('Reviewing step results')
    progress.emit({'id':'phase-process-results','status':'running'})
    assert events[-1]['text'].startswith('Reviewing results and planning the next step')
    assert '1 step completed' in events[-1]['text']
    assert events[-1]['phase_id']=='phase-process-results'
