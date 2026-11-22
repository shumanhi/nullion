"""Request-local progress facts and best-effort transport, shared by all clients.

This builds the existing UserMessage contract. It never interprets prompts,
copies source prose, changes execution, or starts another model call.
"""
from __future__ import annotations

import asyncio
import logging
import threading

from nullion.chat_streaming import LatestTextUpdateQueue

logger = logging.getLogger(__name__)
PROGRESS_KIND = 'request_progress'
OPERATION_KIND = 'operation_progress'
_PROGRESS_PHASE_IDS = frozenset({'prepare', 'source', 'operation', 'phase-check-task-state',
    'phase-load-memory', 'phase-check-attachments', 'phase-select-tools', 'phase-build-context',
    'phase-start-model', 'phase-process-results', 'phase-prepare-artifacts', 'phase-save-conversation',
    'phase-resume-approval'})
SOURCE_ACTIVITY_PHASES = {
    'reading_pages': 'Reading pages.',
    'reviewing_captured_evidence': 'Reviewing captured page details.',
    'recording_findings': 'Recording findings.',
    'verifying_evidence': 'Verifying source details.',
    'reviewing_findings': 'Comparing findings.',
    'finishing_source_review': 'Finishing source review.',
}
# Core native tool identities are presentation metadata, never prompt routing.
TOOL_ACTIVITY_PHASES = {
    'web_search': ('Searching the web.', 'Reviewing search results.'),
    'browser_snapshot': ('Inspecting the current page.', 'Reviewing page details.'),
    'browser_find': ('Finding page controls.', 'Reviewing page controls.'),
    'browser_click_id': ('Opening the selected page control.', 'Checking the updated page.'),
    'browser_click_element': ('Opening the selected page control.', 'Checking the updated page.'),
    'browser_run_js': ('Working with the current page.', 'Reviewing page results.'),
    'email_send': ('Sending the approved email.', 'Checking email delivery.'),
    'code_mode': ('Working through the next steps.', 'Reviewing step results.'),
    'browser_navigate': ('Opening a page.', 'Reviewing page details.'),
    'browser_read_pages': ('Reading pages.', 'Reviewing page details.'),
    'browser_extract_text': ('Reading a page.', 'Reviewing page details.'),
    'browser_extract_items': ('Reading page listings.', 'Reviewing page details.'),
    'browser_extract_detail': ('Reading page details.', 'Reviewing page details.'),
    'browser_run_task': ('Checking the browser.', 'Reviewing findings.'),
    'browser_image_collect': ('Collecting images.', 'Reviewing images.'),
    'file_read': ('Reading a file.', 'Reviewing file contents.'),
}


def _tool_activity_phase(operation, index):
    kind=operation.get('activity_kind')
    return TOOL_ACTIVITY_PHASES.get(kind, ('',''))[index] if isinstance(kind,str) else ''


def tool_operation_progress(result, spec):
    """Safe lifecycle facts from the executing registry, without result prose."""
    from nullion.tools import normalize_tool_status
    if spec is None:
        return None
    status = normalize_tool_status(result.status)
    output = result.output if isinstance(result.output, dict) else {}
    approval_id = output.get('approval_id')
    pending_approval = (status == 'denied' and output.get('requires_approval') is True
        and isinstance(approval_id, str) and bool(approval_id.strip()))
    if result.status == 'approval_required' or pending_approval:
        status = 'approval_required'
    elif status == 'nonterminal':
        status = 'running'
    elif status == 'approved':
        status = 'completed'
    elif status in {'blocked', 'suspended'}:
        status = 'approval_required'
    produces_file = bool(getattr(spec, 'produced_artifact_extensions', ())) and not bool(
        getattr(spec, 'source_artifact_only', False))
    tags = getattr(spec, 'capability_tags', ())
    native_name = getattr(spec, 'name', None)
    from nullion.tools import ToolSideEffectClass
    category = ('retained_evidence' if 'retained_evidence' in tags
        else 'saved_information' if 'conversation_history' in tags
        else 'calendar' if 'calendar' in tags and spec.side_effect_class is ToolSideEffectClass.READ
        else 'mail' if 'email' in tags and spec.side_effect_class is ToolSideEffectClass.READ
        else 'scheduled_tasks' if 'scheduler_inventory' in tags and spec.side_effect_class is ToolSideEffectClass.READ
        else 'information')
    return {'status_kind': OPERATION_KIND, 'operation_progress': {
        'id': result.invocation_id, 'status': status, 'produces_file': produces_file,
        'category': category,
        **({'activity_kind': native_name} if isinstance(native_name,str) and native_name in TOOL_ACTIVITY_PHASES else {}),
    }}


def publish_source_progress(invocation, *, unit, completed, failed, total, records):
    from nullion.tools import tool_execution_cancelled
    if invocation.progress_callback is None or tool_execution_cancelled():
        return
    try:
        invocation.progress_callback({'pages_checked':completed + failed, 'findings_found':records,
            'source_progress':{'unit':unit,'completed':completed,'failed':failed,'total':total,'records':records}})
    except Exception:
        logger.debug('Progress callback failed; preserving observed evidence', exc_info=True)


class RequestProgress:
    def __init__(self, callback):
        self.callback = callback
        self.batches = {}
        self.operations = {}
        self.operation_facts = {}
        self.observations = {}
        self.phase = ''
        self.phase_id = 'prepare'
        self.completion_fact = ''
        self.last_text = ''
        self.lock = threading.RLock()
        self.closed = False

    def emit(self, event):
        with self.lock:
            self._emit(event)

    def _emit(self, event):
        # Preserve verbose activity as its own optional rendering of the same
        # execution. Native counts, not activity labels/prose, establish facts.
        if event.get('status_kind') != OPERATION_KIND:
            self.callback(event)
        if event.get('status_kind') == PROGRESS_KIND and event.get('status') == 'done':
            self.closed = True
        if self.closed:
            return
        progress = event.get('source_progress')
        if isinstance(progress, dict):
            values = [progress.get(k) for k in ('completed', 'failed', 'total', 'records')]
            if (progress.get('unit') in {'pages', 'searches'}
                    and all(type(v) is int and 0 <= v <= 100000 for v in values)
                    and values[0] + values[1] <= values[2]):
                key = event.get('operation_id')
                if isinstance(key, str) and key:
                    self.batches[key] = dict(progress)
                    self.completion_fact = ''
                    self.phase = 'Checking sources.'
                    self.phase_id = 'source'
        operation = event.get('operation_progress')
        observed = event.get('observed_progress')
        if not isinstance(progress, dict) and isinstance(observed, dict):
            key = event.get('operation_id')
            if (isinstance(key, str) and key and all(type(observed.get(k)) is int
                    and 0 <= observed[k] <= 100000 for k in ('pages_checked', 'findings_found'))):
                self.observations[key] = dict(observed)
                self.completion_fact = ''
                phase = event.get('activity_phase')
                self.phase = SOURCE_ACTIVITY_PHASES.get(phase if isinstance(phase, str) else None,
                    'Checking sources.')
                self.phase_id = 'source'
        if isinstance(operation, dict) and isinstance(operation.get('id'), str):
            status = operation.get('status')
            if status in {'completed', 'failed', 'running', 'approval_required', 'denied', 'cancelled'}:
                self.phase_id = 'operation'
                self.operations[operation['id']] = status
                self.operation_facts[operation['id']] = dict(operation)
                active = [row for key,row in self.operation_facts.items()
                          if self.operations.get(key) == 'running']
                # A receipt describes the just-finished activity, not every
                # later activity in the request. Keep cumulative source counts
                # separately; retire this label when real work moves on.
                self.completion_fact = ''
                if status == 'completed' and not active:
                    self.completion_fact = ('File prepared' if operation.get('produces_file') is True
                        else {'calendar': 'Calendar checked', 'mail': 'Mail checked',
                              'scheduled_tasks': 'Scheduled tasks checked'}.get(operation.get('category'), ''))
                if status == 'approval_required':
                    self.phase = 'Waiting for your approval.'
                elif active:
                    self.phase = ('Preparing the requested file.' if any(row.get('produces_file') for row in active)
                        else 'Reviewing captured evidence.' if any(row.get('category')=='retained_evidence' for row in active)
                        else 'Searching saved information.' if any(row.get('category')=='saved_information' for row in active)
                        else next((label for row in reversed(active) if (label:=_tool_activity_phase(row,0))),
                            'Checking information.'))
                else:
                    # Completed counts are the progress fact. Do not cycle a
                    # generic "working with results" label after every tool.
                    self.phase = ('Reviewing saved information.' if status == 'completed' and operation.get('category') == 'saved_information'
                        else 'Reviewing captured evidence.' if status == 'completed' and operation.get('category') == 'retained_evidence'
                        else (_tool_activity_phase(operation,1) or
                            ('Reviewing findings.' if self.batches or self.observations else 'Reviewing step results.')) if status == 'completed'
                        else 'Checking the next step.')
        from nullion.chat_text import WORKING_ACK_SUMMARY
        phases = {
            'prepare': WORKING_ACK_SUMMARY,
            'phase-resume-approval': 'Continuing the approved action.',
            'phase-check-task-state': 'Checking task context.',
            'phase-load-memory': 'Loading saved context.',
            'phase-check-attachments': 'Checking attachments.',
            'phase-select-tools': 'Preparing the tools for your request.',
            'phase-build-context': 'Preparing request context.',
            'phase-start-model': 'Planning the next step.',
            'phase-process-results': 'Reviewing results and planning the next step.',
            'phase-prepare-artifacts': 'Preparing your response.',
            'phase-save-conversation': 'Finishing your response.',
        }
        accepted = event.get('id') == 'prepare' and event.get('status') == 'done'
        if (event.get('status') == 'running' or accepted) and event.get('id') in phases:
            self.phase = phases[event['id']]
            self.phase_id = event['id']
            if event['id'] != 'phase-process-results':
                self.completion_fact = ''
        if not self.phase and not self.operations:
            return
        from nullion.chat_text import UserMessage, render_user_message
        facts = []
        for unit, singular, plural in (('searches', 'search', 'searches'), ('pages', 'page read', 'pages read')):
            if unit == 'pages' and self.observations:
                continue  # Adaptive page checks provide the more current count.
            rows = [r for r in self.batches.values() if r['unit'] == unit]
            completed = sum(r['completed'] for r in rows)
            failed = sum(r['failed'] for r in rows)
            total = sum(r['total'] for r in rows)
            if rows and completed:
                pending = completed + failed < total
                count = f'{completed}/{total}' if pending else str(completed)
                facts.append(f'{count} {singular if completed == 1 and not pending else plural}')
        if self.observations:
            pages = sum(r['pages_checked'] for r in self.observations.values())
            if pages:
                facts.append(f'{pages} page' + ('s' if pages != 1 else '') + ' checked')
        records = sum(r['records'] for r in self.batches.values() if r['unit']=='searches')
        if records:
            facts.insert(1, f'{records} result' + ('s' if records != 1 else ''))
        if self.completion_fact:
            facts.append(self.completion_fact)
        if not facts:
            completed = sum(v == 'completed' for v in self.operations.values())
            if completed:
                facts.append(f'{completed} step' + ('s' if completed != 1 else '') + ' completed')
        text = render_user_message(UserMessage(summary=' · '.join(
            [part for part in (self.phase.removesuffix('.'), *facts[:4]) if part])
            or 'Working on your request', kind='update'))
        if text != self.last_text:
            self.last_text = text
            self.callback({'id': 'request-progress', 'label': 'Progress', 'status_kind': PROGRESS_KIND,
                'status': 'running', 'text': text, 'completion_scope': 'operation', 'phase_id':self.phase_id})


def approval_resume_progress(activity_callback, registry):
    """Use the same native phase and tool facts as a regular chat turn."""
    if activity_callback is None:
        return {}, lambda: None
    progress = RequestProgress(activity_callback)

    def report_tool(result):
        try:
            spec = registry.get_spec(result.tool_name)
        except KeyError:
            return
        event = tool_operation_progress(result, spec)
        if event:
            progress.emit(event)

    progress.emit({'id': 'phase-resume-approval', 'label': 'Continuing the approved action', 'status': 'running'})
    return {
        'foreground_event_callback': progress.emit,
        'tool_result_callback': report_tool,
    }, lambda: progress.emit({'id': 'phase-save-conversation', 'label': 'Finishing your response', 'status': 'running'})


class RequestProgressDelivery:
    """One editable notice; coalesced, delayed for fast turns, never final output.

    A failed status transport cannot block execution/final delivery or cause a
    second speculative send. Closing stops updates and removes the notice.
    """
    def __init__(self, *, loop, send, edit, remove, delay=2.5, interval=1.5, can_publish=None,
                 on_published=None):
        self.send, self.edit, self.remove = send, edit, remove
        self.handle = None
        self.published_text = None
        self.closed = threading.Event()
        self.loop = loop
        self.can_publish = can_publish
        self.on_published = on_published
        self.phases = {}
        self.phase_lock = threading.Lock()
        self.acceptance_text = None
        self.latest_text = None
        self.queue = LatestTextUpdateQueue(loop, self._publish, interval=interval, initial_delay=delay)
        self.cleanup = None
        self.drain_task = None

    def emit(self, event):
        if event.get('status_kind') == PROGRESS_KIND and event.get('status') == 'done':
            if not self.closed.is_set():
                self.closed.set()
                asyncio.run_coroutine_threadsafe(self.finish(), self.loop)
            return
        if (not self.closed.is_set() and event.get('status_kind') == PROGRESS_KIND
                and isinstance(event.get('text'), str) and event['text'].strip()):
            # Keep only the bounded identity of queued views. The observer
            # receives the phase actually acknowledged by the transport.
            with self.phase_lock:
                if len(self.phases) >= 32:
                    self.phases.pop(next(iter(self.phases)))
                phase = event.get('phase_id')
                self.phases[event['text']] = phase if isinstance(phase, str) and phase in _PROGRESS_PHASE_IDS else None
                if phase == 'prepare' and self.acceptance_text is None:
                    self.acceptance_text = event['text']
                self.latest_text = event['text']
                self.queue.submit(event['text'])

    async def _publish(self, text):
        from nullion.turn_dispatch_graph import TurnPublicationRevoked
        if self.closed.is_set():
            return
        try:
            if self.can_publish is not None and not self.can_publish():
                self.closed.set()
                return
            if text == self.published_text:
                return
            operation = 'send' if self.handle is None else 'edit'
            acceptance = False
            if self.handle is None:
                with self.phase_lock:
                    phase = self.phases.get(text)
                    if self.acceptance_text and phase in _PROGRESS_PHASE_IDS - {'operation', 'source', 'phase-process-results'}:
                        # Preserve the initial acceptance copy on slow prep.
                        # Actual source/tool facts can still be the first view.
                        text = self.acceptance_text
                        acceptance = True
            if self.handle is None:
                self.handle = await self.send(text)
                if self.handle is None:
                    self.closed.set()
            elif not await self.edit(self.handle, text):
                return
            if self.handle is not None and callable(self.on_published):
                with self.phase_lock:
                    phase = 'prepare' if acceptance else self.phases.get(text)
                try:
                    self.on_published({'operation':operation, 'phase_id':phase or 'update'})
                except Exception:
                    logger.debug('Progress timing observer failed', exc_info=True)
            if not self.closed.is_set():
                self.published_text = text
                if acceptance:
                    with self.phase_lock:
                        if self.latest_text != text:
                            # A newer view may have arrived during the send.
                            # Never overwrite it with the older captured phase.
                            self.queue.submit(self.latest_text)
        except TurnPublicationRevoked:
            self.closed.set()
        except Exception:
            # An initial send may have been accepted without returning a
            # receipt, so never resend it. Edits have an owned receipt and are
            # safe to recover on the next update, without another message.
            if self.handle is None:
                self.closed.set()
            logger.debug('Progress transport unavailable; final delivery remains independent', exc_info=True)

    def _close(self):
        self.closed.set()
        if self.drain_task is None:
            self.drain_task = asyncio.create_task(self.queue.drain())

    async def freeze(self):
        """Stop new updates; retain an existing notice until delivery finishes."""
        self._close()
        try:
            await asyncio.wait_for(asyncio.shield(self.drain_task), timeout=.1)
        except TimeoutError:
            pass
        except Exception:
            logger.debug('Progress queue drain failed', exc_info=True)

    async def finish(self):
        self._close()
        if self.cleanup is None:
            async def cleanup():
                try:
                    await self.drain_task
                except Exception:
                    logger.debug('Progress queue drain failed', exc_info=True)
                if self.handle is not None:
                    try:
                        await self.remove(self.handle)
                    except Exception:
                        logger.debug('Progress notice cleanup failed', exc_info=True)
            self.cleanup = asyncio.create_task(cleanup())
        # A stuck edit/upload must not delay the requested answer.
        try:
            await asyncio.wait_for(asyncio.shield(self.cleanup), timeout=.1)
        except TimeoutError:
            pass
