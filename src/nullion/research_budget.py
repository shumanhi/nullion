"""Request-owned research accounting and a bounded transition into writing.

Only registered tool metadata and validated primary scope decisions activate
this policy. Prompt text is never classified here. Native receipts stay exact.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from contextvars import copy_context
from copy import deepcopy
from dataclasses import dataclass, field, replace
from functools import wraps
import inspect
import json
import math
import os
import threading
import time

_current = ContextVar('research_budget', default=None)
_EFFORT = {'standard': 1, 'extended': 2, 'exhaustive': 4}


def _number(name, default):
    try:
        value = float(os.environ.get(name, default))
        return max(1., value) if math.isfinite(value) else float(default)
    except (TypeError, ValueError):
        return float(default)


def is_research_tool(spec):
    if spec is None or str(getattr(spec.side_effect_class, 'value', spec.side_effect_class)) != 'read':
        return False
    from nullion.turn_context_policy import tool_uses_external_observation
    return tool_uses_external_observation(spec) or bool(set(spec.capability_tags).intersection({'web', 'browser', 'account_read'}))


def _specs(registry):
    method = getattr(registry, 'list_specs', None) or getattr(registry, 'all_specs', None)
    return method() if callable(method) else ()


@dataclass
class ResearchBudget:
    effort: str = 'standard'
    questions: tuple[str, ...] = ()
    active: bool = False
    started: float | None = None
    writing_started: float | None = None
    reason: str | None = None
    native_calls: int = 0
    model_calls: int = 0
    writing_calls: int = 0
    cache_hits: int = 0
    writer_forced_stop: bool = False
    source_names: set = field(default_factory=set)
    required_sources: set[str] = field(default_factory=set)
    attempted_sources: set[str] = field(default_factory=set)
    completed_sources: set[str] = field(default_factory=set)
    reads: dict = field(default_factory=dict)
    local_operations: set = field(default_factory=set)
    paused_at: float | None = None
    lock: object = field(default_factory=threading.RLock, repr=False)

    def configure(self, registry, decision=None):
        with self.lock:
            # Scope may be widened during recovery; it cannot reset spent work
            # or repeatedly upgrade a running request's allowance.
            if self.native_calls == 0 and self.writing_started is None and decision is not None and getattr(decision, 'valid', False):
                effort = getattr(decision, 'research_effort', 'standard')
                self.effort = effort if effort in _EFFORT else 'standard'
                self.questions = tuple(getattr(decision, 'research_questions', ()) or ())
                selected = set(getattr(decision, 'requested_tool_names', ()) or ())
                selected.update(getattr(decision, 'required_tool_names', ()) or ())
                for name in selected:
                    try:
                        if is_research_tool(registry.get_spec(name)):
                            self.activate()
                            break
                    except (AttributeError, KeyError):
                        continue
            if self.active:
                self.register_sources(registry)
                if getattr(decision, 'valid', False):
                    self.required_sources.update(set(getattr(decision, 'required_tool_names', ())) & self.source_names)

    def source_reservation(self, tool_name):
        if len(self.required_sources) < 2:
            return [], 0.0
        outstanding = (self.required_sources - self.attempted_sources) - {tool_name}
        reserved = self.limits()['seconds'] * .3 if outstanding else 0.0
        if tool_name in self.required_sources and tool_name not in self.completed_sources:
            # A first or still unsuccessful required read may be the
            # prerequisite for another. Never reserve away its entire attempt.
            reserved = min(reserved, self.remaining() * .4)
        return sorted(outstanding), reserved

    def register_sources(self, registry):
        specs = _specs(registry)
        with self.lock:
            self.source_names.update(spec.name for spec in specs if is_research_tool(spec))

    def activate(self):
        if not self.active:
            self.active = True
            self.started = time.monotonic()

    def limits(self):
        scale = _EFFORT[self.effort]
        return {'native_calls': int(_number('NULLION_RESEARCH_TOOL_CALLS', 24)*scale),
                'model_calls': int(_number('NULLION_RESEARCH_MODEL_CALLS', 8)*scale),
                'seconds': _number('NULLION_RESEARCH_SECONDS', 75)*scale}

    def finish_research(self, reason):
        if self.writing_started is None:
            self.reason = reason
            self.writing_started = time.monotonic()

    def remaining(self):
        if not self.active:
            return None
        return max(0., self.limits()['seconds']-((self.paused_at or time.monotonic())-self.started))

    def writing(self):
        with self.lock:
            if not self.active:
                return False
            limits = self.limits()
            if self.native_calls >= limits['native_calls']:
                self.finish_research('native_call_limit')
            elif self.model_calls >= limits['model_calls']:
                self.finish_research('model_call_limit')
            elif self.remaining() <= 0:
                self.finish_research('research_deadline')
            return self.writing_started is not None

    def writing_remaining(self):
        if not self.writing():
            return None
        now = max(self.writing_started, self.paused_at or time.monotonic())
        return max(0., _number('NULLION_RESEARCH_WRITING_SECONDS', 60)-(now-self.writing_started))

    def exhausted_writer(self):
        return self.writing() and (self.writing_calls >= int(_number('NULLION_RESEARCH_WRITING_CALLS', 8))
                                   or self.writing_remaining() <= 0)

    def prompt(self):
        if not self.active:
            return ''
        writing = self.writing()
        return ('\n\nRuntime research state (authoritative): ' + json.dumps({
            'phase': 'writing' if writing else 'research', 'effort': self.effort,
            'native_calls': self.native_calls, 'model_calls': self.model_calls,
            'remaining_seconds': round(self.writing_remaining() if writing else self.remaining(), 1),
            'limits': self.limits(), 'required_questions': list(self.questions), 'reason': self.reason,
            'unattempted_required_source_tools': sorted(self.required_sources - self.attempted_sources)}) +
            ('\nResearch is closed. Compose the answer from completed evidence. State unresolved required facts explicitly; '
             'do not claim complete coverage from a budget limit. Retained inspection and requested local artifacts/actions '
             'remain available, but do not search, fetch or widen research again.' if writing else
             '\nResolve only material questions of the original request. Return a useful supported answer as soon as they '
             'are resolved. Start independently callable required source tools before repeated reads of one source; '
             'honor data dependencies when a later tool needs an earlier result. '
             'Additional alternatives, media or background are optional unless the user required them.'))

    def usage(self):
        if not self.active:
            return {}
        return {'research_native_calls': self.native_calls, 'research_model_calls': self.model_calls,
                'research_writing_calls': self.writing_calls, 'research_cache_hits': self.cache_hits,
                'research_writing_phase': int(self.writing()), 'research_effort_scale': _EFFORT[self.effort],
                'research_elapsed_ms': int(((self.writing_started or time.monotonic())-self.started)*1000),
                'research_writing_elapsed_ms': int((time.monotonic()-self.writing_started)*1000) if self.writing_started else 0}

    def restore(self, usage):
        if self.active or not usage.get('research_effort_scale'):
            return
        self.active = True
        self.effort = next((key for key, scale in _EFFORT.items() if scale == usage['research_effort_scale']), 'standard')
        for attr in ('native_calls', 'model_calls', 'writing_calls', 'cache_hits'):
            setattr(self, attr, max(0, int(usage.get('research_'+attr, 0))))
        elapsed = max(0, int(usage.get('research_elapsed_ms', 0)))/1000
        if usage.get('research_writing_phase'):
            self.writing_started = time.monotonic()-max(0, int(usage.get('research_writing_elapsed_ms', 0)))/1000
            self.started = self.writing_started-elapsed
            self.reason = 'retained_research_limit'
        else:
            self.started = time.monotonic()-elapsed


def current_budget():
    return _current.get()


@contextmanager
def research_context():
    if current_budget() is not None:
        yield current_budget()
        return
    token = _current.set(ResearchBudget())
    try:
        yield current_budget()
    finally:
        _current.reset(token)


def research_scope(function):
    if inspect.iscoroutinefunction(function):
        @wraps(function)
        async def asynchronous(*args, **kwargs):
            with research_context():
                return await function(*args, **kwargs)
        return asynchronous
    @wraps(function)
    def synchronous(*args, **kwargs):
        with research_context():
            return function(*args, **kwargs)
    return synchronous


def prepare_model(state):
    budget = current_budget()
    if budget is None:
        return None
    registry = state.get('tool_registry')
    if state.get('research_resume') is True:
        budget.restore(state.get('work_usage') or {})
    budget.configure(registry, getattr(registry, 'turn_tool_scope_decision', None))
    state.setdefault('work_usage', {}).update(budget.usage())
    return budget if budget.active else None


def prepare_model_request(kwargs, budget=None, *, production=False):
    """Apply the same budget at each actual model attempt, including retries."""
    budget = budget or current_budget()
    if budget is None or not budget.active:
        return
    with budget.lock:
        if budget.model_calls + 1 >= budget.limits()['model_calls']:
            budget.finish_research('model_call_limit')
        writing = budget.writing()
        if writing:
            budget.writing_calls += 1
            kwargs['tools'] = [tool for tool in kwargs.get('tools', ()) if tool.get('name') not in budget.source_names]
            remaining = budget.writing_remaining()
        else:
            budget.model_calls += 1
            remaining = budget.remaining()
        if not production and isinstance(kwargs.get('timeout'), (int, float)):
            kwargs['timeout'] = max(.1, min(kwargs['timeout'], remaining))
        kwargs['system'] = str(kwargs.get('system') or '') + budget.prompt()


def _cache_key(spec, invocation):
    context = invocation.flow_context or {}
    bindings = {key: context.get(key) for _, key in getattr(spec, 'context_argument_bindings', ())}
    bindings.update({key: context.get(key) for key in getattr(spec, 'read_cache_context_keys', ())})
    return json.dumps([id(spec), invocation.principal_id, spec.name, invocation.arguments, bindings],
        sort_keys=True, default=str)


def before_tool(spec, invocation):
    """Return a cached/denied receipt or admit exactly one native operation."""
    budget = current_budget()
    if budget is None:
        return None
    from nullion.tools import ToolResult
    with budget.lock:
        if getattr(spec, 'invalidates_read_cache', False) or str(spec.side_effect_class.value) != 'read':
            budget.reads.clear()
        source = is_research_tool(spec)
        if source:
            budget.activate()
            budget.source_names.add(spec.name)
        key = _cache_key(spec, invocation)
        if spec.same_request_read_cache and key in budget.reads:
            cached = budget.reads[key]
            budget.cache_hits += 1
            return ToolResult(invocation.invocation_id, invocation.tool_name, cached.status, deepcopy(cached.output),
                response_requirements={**{key: value for key, value in (cached.response_requirements or {}).items()
                    if key != '_native_completed_reads'}, '_native_reused_observation': {
                    'origin_invocation_id': cached.invocation_id, 'same_request': True, 'external_read_executed': False}})
        if source:
            batch = getattr(spec, 'independent_batch', ())
            entries = invocation.arguments.get(batch[0]) if batch else None
            units = len(entries) if isinstance(entries, list) else 1
            outstanding, reserve = budget.source_reservation(spec.name)
            priority_read = spec.name in budget.required_sources and spec.name not in budget.completed_sources
            if (outstanding and not priority_read and not budget.writing()
                    and (budget.remaining() <= reserve
                         or budget.native_calls + units + len(outstanding) > budget.limits()['native_calls'])):
                return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
                    'reason': 'research_reserved_for_required_sources', 'executed': False,
                    'unattempted_required_source_tools': outstanding},
                    error='The remaining research allowance is reserved for required source tools that have not run.')
            if budget.native_calls + units > budget.limits()['native_calls']:
                budget.finish_research('native_call_limit')
            if budget.writing():
                return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', {
                    'reason': 'research_budget_exhausted', 'executed': False, 'failure_scope': 'request',
                    'research': budget.usage(), 'next_phase': 'writing'},
                    error='Research is closed; compose the answer from retained evidence and disclose remaining gaps.')
            budget.native_calls += units
            budget.attempted_sources.add(spec.name)
        elif budget.active:
            if not budget.local_operations:
                budget.paused_at = time.monotonic()
            budget.local_operations.add(invocation.invocation_id)
    return None


def after_tool(spec, invocation, result):
    budget = current_budget()
    if budget is None:
        return
    with budget.lock:
        if invocation.invocation_id in budget.local_operations:
            budget.local_operations.discard(invocation.invocation_id)
            if not budget.local_operations and budget.paused_at is not None:
                now = time.monotonic()
                budget.started += now-budget.paused_at
                if budget.writing_started is not None:
                    budget.writing_started += now-max(budget.writing_started, budget.paused_at)
                budget.paused_at = None
            if (result.status == 'completed' and spec.side_effect_class.value == 'write'
                    and budget.writing_started is not None):
                # Productive requested work can span several artifact steps.
                # It never reopens discovery or resets the research counters.
                budget.writing_calls = 0
                budget.writing_started = time.monotonic()
                if budget.local_operations:
                    budget.paused_at = time.monotonic()
        if result.status == 'completed' and spec.name in budget.source_names:
            budget.completed_sources.add(spec.name)
        if spec.same_request_read_cache and result.status == 'completed':
            key = _cache_key(spec, invocation)
            budget.reads[key] = deepcopy(result)


def source_timeout(spec):
    budget = current_budget()
    if budget is None or not budget.active or not is_research_tool(spec):
        return None
    with budget.lock:
        _, reserved = budget.source_reservation(spec.name)
        return max(0.0, budget.remaining() - reserved)


class _FixedResearchScope:
    """The existing scope control for workflows whose tool set is already fixed.

    This view cannot expose additional tools or grant source/account access.
    It records only a primary-produced plan over the workflow's existing tools.
    """
    def __init__(self, delegate, decision=None):
        self._delegate = delegate
        # Research effort is not a replacement authorization/scheduler scope.
        # Preserve the delegate's scope identity (or absence) through __getattr__.
        self.research_scope_decision = decision

    def get_spec(self, name):
        if name == 'request_tool_scope':
            from nullion.turn_context_policy import _SCOPE_REQUEST_TOOL_SPEC
            schema = deepcopy(_SCOPE_REQUEST_TOOL_SPEC.input_schema)
            schema['required'] = list(dict.fromkeys([*schema.get('required', ()), 'research_effort']))
            if getattr(self._delegate, 'scheduled_task_execution_registry', False):
                from nullion.scheduled_artifact_contract import deliverables_schema
                schema['properties']['artifact_deliverables'] = deliverables_schema()
            available = sorted(spec.name for spec in _specs(self._delegate))
            for key in ('tool_names', 'required_tool_names'):
                schema['properties'][key]['items'] = {'type':'string', 'enum':available}
            return replace(_SCOPE_REQUEST_TOOL_SPEC, input_schema=schema, description=
                'Configure the research allowance over this workflow\'s already selected tools. '
                'Declare research_effort and material research_questions before '
                'the first source read. In scheduled runs, declare final artifact_extensions and '
                'artifact_deliverables separately from supporting files. '
                'This control cannot expose additional tools or change permissions.')
        return self._delegate.get_spec(name)

    def all_specs(self):
        return [*_specs(self._delegate), self.get_spec('request_tool_scope')]

    def list_specs(self):
        return self.all_specs()

    def list_tool_definitions(self, *args, **kwargs):
        spec = self.get_spec('request_tool_scope')
        return [{'name':spec.name, 'description':spec.description, 'input_schema':spec.input_schema,
                 'capability_tags':list(spec.capability_tags), 'side_effect_class':'read', 'requires_approval':False},
                *self._delegate.list_tool_definitions(*args, **kwargs)]

    def can_invoke_tool(self, name):
        return name == 'request_tool_scope' or self._delegate.can_invoke_tool(name)

    def apply_scope_request(self, invocation):
        from jsonschema import validate, ValidationError
        from nullion.tools import ToolResult
        from nullion.turn_context_policy import TurnToolScopeDecision, _research_scope_values
        from nullion.artifacts import normalize_artifact_extensions
        from nullion.attachment_format_graph import VALID_ATTACHMENT_EXTENSIONS
        from nullion.response_fulfillment_contract import normalize_artifact_media_required_extensions
        from nullion.scheduled_artifact_contract import extension_requirements, normalize_deliverables
        try:
            validate(invocation.arguments, self.get_spec('request_tool_scope').input_schema)
            names = tuple(invocation.arguments.get('tool_names') or ())
            required = tuple(invocation.arguments.get('required_tool_names') or ())
            available = {spec.name for spec in _specs(self._delegate)}
            if not set(names).union(required).issubset(available):
                raise ValueError('This workflow cannot expose additional tools')
            requested_extensions = normalize_artifact_extensions(invocation.arguments.get('artifact_extensions', ()))
            excluded = normalize_artifact_extensions(invocation.arguments.get('excluded_artifact_extensions', ()))
            if not set(requested_extensions + excluded).issubset(VALID_ATTACHMENT_EXTENSIONS):
                raise ValueError('The workflow declared an unsupported artifact format')
            rows = (normalize_deliverables(invocation.arguments['artifact_deliverables'])
                    if 'artifact_deliverables' in invocation.arguments else None)
            if rows is not None and any(row['extension'] in excluded for row in rows):
                raise ValueError('A deliverable cannot use an excluded format')
            requested_extensions = tuple(dict.fromkeys((*requested_extensions, *extension_requirements(rows or ()))))
            requested_extensions = tuple(extension for extension in requested_extensions if extension not in excluded)
            embedded = normalize_artifact_media_required_extensions([
                *(invocation.arguments.get('embedded_media_artifact_extensions') or ()),
                *(invocation.arguments.get('required_embedded_media_extensions') or ())])
            embedded = tuple(extension for extension in embedded if extension in requested_extensions)
        except (ValidationError, ValueError, TypeError) as error:
            return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
                {'reason':'invalid_tool_arguments','executed':False}, error=str(error)), self
        decision = TurnToolScopeDecision(valid=True, requested_tool_names=names, required_tool_names=required,
            requested_artifact_extensions=requested_extensions, excluded_artifact_extensions=excluded,
            required_embedded_media_extensions=embedded, **_research_scope_values(invocation.arguments))
        view = _FixedResearchScope(self._delegate, decision)
        if (budget := current_budget()) is not None:
            budget.configure(view, decision)
            if invocation.arguments.get('research_refresh') is True:
                budget.reads.clear()
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', {
            'scope_requested':True, 'research_effort':decision.research_effort,
            'research_questions':list(decision.research_questions), 'available_tools':sorted(available),
            'required_tool_names':list(required), 'fixed_tool_set':True,
            'artifact_extensions':list(requested_extensions), 'excluded_artifact_extensions':list(excluded),
            'embedded_media_artifact_extensions':list(embedded),
            **({'artifact_deliverables':list(rows)} if rows is not None else {})}), view

    def invoke(self, invocation):
        if invocation.tool_name == 'request_tool_scope':
            return self.apply_scope_request(invocation)[0]
        return self._delegate.invoke(invocation)

    def __getattr__(self, name):
        return getattr(self._delegate, name)


def ensure_research_scope(registry):
    if callable(getattr(registry, 'apply_scope_request', None)):
        return registry
    return _FixedResearchScope(registry) if any(is_research_tool(spec) for spec in _specs(registry)) else registry


def call_model(create, kwargs, *, cancellation_checker=None, production=False, time_limit=None):
    """Bound active research even when a provider ignores its timeout option."""
    budget = current_budget()
    if budget is None or not budget.active:
        return create(**kwargs)
    remaining = budget.writing_remaining() if budget.writing() else budget.remaining()
    limit = float(kwargs.get('timeout') or remaining)
    if time_limit is not None:
        limit = min(limit, time_limit) if kwargs.get('timeout') else time_limit
    deadline = max(.001, limit if production else min(limit, remaining))
    started = time.monotonic()
    done, values = threading.Event(), []
    context = copy_context()
    def run():
        try:
            values.append((True, context.run(create, **kwargs)))
        except BaseException as error:
            values.append((False, error))
        finally:
            done.set()
    threading.Thread(target=run, name='research-model-call', daemon=True).start()
    end = time.monotonic()+deadline
    while not done.wait(min(.05, max(.001, end-time.monotonic()))):
        if callable(cancellation_checker) and cancellation_checker():
            from concurrent.futures import CancelledError
            raise CancelledError('request_cancelled')
        if time.monotonic() >= end:
            # An attempt timeout can be retried within the same request budget.
            # Do not retire source tools until the shared allowance is spent.
            if budget.remaining() <= 0:
                budget.finish_research('model_deadline')
            raise TimeoutError('The active research model call exceeded its remaining allowance')
    success, value = values[0]
    if not success:
        raise value
    if production and isinstance(value, dict):
        calls = [block for block in value.get('content', ()) if isinstance(block, dict) and block.get('type') == 'tool_use']
        names = {block.get('name') for block in calls}
        for block in calls:
            if block.get('name') == 'code_mode':
                names.update((block.get('input') or {}).get('tool_names') or ())
        if calls and not names.intersection(budget.source_names):
            # Artifact-capable calls keep their registered generation timeout.
            # Only a verified non-source tool selection earns this time back;
            # choosing another read still consumes the research allowance.
            now = time.monotonic()
            with budget.lock:
                budget.started += now-started
                if budget.writing_started is not None:
                    budget.writing_started += now-max(started, budget.writing_started)
    return value
