"""Agent-facing contracts for temporary live observations."""

from datetime import timedelta
import hashlib
import json
from pathlib import Path
import os

from nullion.live_tracking import (
    LiveTrackerStore,
    DEFAULT_INTERVAL,
    DEFAULT_SOURCE_REFRESH,
    MIN_INTERVAL,
    MAX_INTERVAL,
    tracking_deadline,
    TrackingDeadlineError,
    LiveTrackerPlanError,
    STRUCTURED_OPERATORS,
    public_tracker,
    utcnow,
    local_time,
    validate_plan,
    validate_prepared_observations,
    evaluate_semantic,
    observation_problem,
    EVALUATION_TIMEOUT_SECONDS,
)
from nullion.tools import (
    ToolInvocation,
    ToolResult,
    ToolSpec,
    ToolRiskLevel,
    ToolSideEffectClass,
    _action_receipt,
)

TRACKING_CHOICE = (
    "Should this be temporary live tracking or a recurring schedule?\n\n"
    "1. Live tracking — frequent checks for new changes until an end time.\n"
    "2. Recurring schedule — checks that continue on a regular schedule.\n"
    "Reply with a number and how long or how often to check."
)
DURATION_CHOICE = (
    "How long should I keep this live tracker running?\n\n"
    "1. 30 minutes.\n2. 1 hour.\n3. 2 hours.\n"
    "You can also give any other finite duration or a future end time with its time zone. It keeps checking for new results until that time."
)



def _deferred_execution_receipt(job, duration_seconds=None, accepted_at=None):
    return {"version": 1, "phase": "accepted", "owner": "live_tracking", "handle": job["id"],
        "configuration": {"interval_seconds": job["interval_seconds"], "expires_at": job["expires_at"],
            "accepted_at": accepted_at or job.get('window_accepted_at') or job['created_at'],
            "deadline_kind": 'duration' if duration_seconds is not None else 'absolute',
            **({"duration_seconds": duration_seconds} if duration_seconds is not None else {}),
            "matcher": job["matcher"], "condition_revision": job.get("condition_revision", 0), "stop_after_match": job.get('stop_after_match', True), "notify_on_expiry": job["notify_on_expiry"]}}

def tracker_store_for_registry(registry):
    path = getattr(registry, "live_tracker_store_path", None)
    if path is None:
        path = (
            Path(os.environ.get("NULLION_HOME", str(Path.home() / ".nullion")))
            / "runtime.db"
        )
    return LiveTrackerStore(path)


def clarification_reply(results):
    for result in reversed(list(results or ())):
        if result.tool_name in {"create_live_tracker", "resume_live_tracker", "update_live_tracker"} and result.status == "completed":
            return None
        output = result.output if isinstance(result.output, dict) else {}
        if output.get("reason") == "live_tracking_needs_details":
            return output.get("clarification") or DURATION_CHOICE
    return None


def _conversation(invocation):
    from nullion.reminders import current_reminder_chat_id

    conversation = (
        (invocation.flow_context or {}).get("conversation_id")
        or current_reminder_chat_id()
        or invocation.principal_id
    )
    if (
        not isinstance(conversation, str)
        or conversation.split(":", 1)[0] not in {"web", "telegram", "slack", "discord"}
        or ":" not in conversation
    ):
        raise ValueError(
            "Live tracking needs the current chat as its alert destination"
        )
    from nullion.connections import workspace_id_for_principal

    if workspace_id_for_principal(conversation) != workspace_id_for_principal(
        invocation.principal_id
    ):
        raise ValueError(
            "The live tracker destination must belong to the current workspace"
        )
    return conversation


def register_live_tracker_tools(registry, *, model_client_getter=None):
    # Entry points may attach the current model after core tools are registered.
    # Existing handlers must see that binding too; re-registration is idempotent.
    if model_client_getter is not None:
        registry.live_tracker_model_client_getter = model_client_getter

    def creation_review_context(invocation, retained_results):
        """Project exact prepared bindings without IO, execution or permission."""
        from jsonschema import validate, ValidationError
        from nullion.observation_replay import validated_observation_setup

        args, context = invocation.arguments, invocation.flow_context or {}
        facts = {'validated': False, 'advisory_concern_scopes': []}
        result = {'native_contract_validation': facts}
        try:
            validate(args, schemas['create_live_tracker'])
            _conversation(invocation)
            tracking_deadline(duration_seconds=args.get('duration_seconds'), expires_at=args.get('expires_at'))
            probe, matcher = validate_plan(args.get('probe'), args.get('matcher'), registry)
            prepared = context.get('prepared_observations')
            if not isinstance(prepared, dict):
                return result
            validate_prepared_observations(probe, matcher, prepared, context.get('observation_preparations'))
            bindings = []
            for index, step in enumerate(probe):
                signature = json.dumps(step, ensure_ascii=False, sort_keys=True, default=str)
                observed = prepared[signature]
                setup = validated_observation_setup(registry, step['tool_name'], observed)
                bindings.append({'step': index, 'tool_name': step['tool_name'], 'prepared_exact_arguments': True,
                    'argument_session_alias': step['arguments'].get('session_id'),
                    'observed_physical_session': observed.get('session_id') if isinstance(observed, dict) else None,
                    'worker_replays_source_recipe': bool(setup)})
            defaults = {key: prop['default'] for key, prop in schemas['create_live_tracker']['properties'].items()
                        if 'default' in prop and args.get(key, prop['default']) == prop['default']}
            facts.update(validated=True, advisory_concern_scopes=['native_contract'] +
                         (['declared_default'] if defaults else []), prepared_bindings=bindings,
                         declared_defaults=defaults, grants_consent=False,
                         runtime_admission_still_required=True)
        except (ValueError, KeyError, TypeError, ValidationError):
            # Native execution supplies the actionable argument-repair contract;
            # an absent/invalid proof never makes a model veto advisory.
            pass
        return result

    def invoke(invocation):
        from nullion.connections import workspace_id_for_principal

        workspace = workspace_id_for_principal(invocation.principal_id)
        args = invocation.arguments
        try:
            store = tracker_store_for_registry(registry)
            if invocation.tool_name == "list_live_trackers":
                jobs = [
                    public_tracker(j)
                    for j in store.list(
                        workspace,
                        include_finished=args.get("include_finished", True),
                        query=args.get("query"),
                    )
                ]
                return ToolResult(
                    invocation.invocation_id,
                    invocation.tool_name,
                    "completed",
                    {
                        "result_kind": "live_tracker_list",
                        "inventory_scope": {"scheduler_kind": "live_tracker", "coverage": "single_kind_only",
                            "include_finished": args.get("include_finished", True), "query": args.get("query")},
                        "trackers": jobs,
                        "message": (
                            "No live trackers."
                            if not jobs
                            else "\n\n".join(
                                f"{i}. {j['name']} — {j['state']}; checks every {j['interval_seconds']} seconds; ends {local_time(j['expires_at'])}."
                                for i, j in enumerate(jobs, 1)
                            )
                        ),
                    },
                )
            if invocation.tool_name == 'update_live_tracker':
                ident = args.get('id')
                if not isinstance(ident,str) or not ident:
                    raise ValueError('Choose a live tracker from list_live_trackers first')
                duration = args.get('duration_seconds')
                if duration is None and args.get('expires_at') is None and args.get('interval_seconds') is None and args.get('stop_after_match') is None and args.get('matcher') is None:
                    return ToolResult(invocation.invocation_id,invocation.tool_name,'failed',
                        {'reason':'live_tracking_needs_details','clarification':DURATION_CHOICE},'Choose the new tracking window')
                saved = store.get(ident, workspace)
                if saved is None or saved['conversation_id'] != _conversation(invocation):
                    raise ValueError('Amend this tracker in its original chat')
                matcher = None
                if args.get('matcher') is not None:
                    try:
                        _, matcher = validate_plan(saved['probe'], args['matcher'], registry)
                    except LiveTrackerPlanError:
                        raise
                    except (ValueError, KeyError, TypeError) as exc:
                        raise LiveTrackerPlanError(str(exc), code='invalid_observation_plan') from exc
                    # Source verification is unchanged. A filter-only edit must
                    # not discard browser state or require redundant source IO.
                request_key = (invocation.capsule_id or invocation.invocation_id)+':update:'+ident+':' + hashlib.sha256(
                    json.dumps(args,sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:16]
                job = store.update_window(ident,workspace,_conversation(invocation),request_key=request_key,
                    duration_seconds=duration,expires_at=args.get('expires_at'),interval_seconds=args.get('interval_seconds'),
                    stop_after_match=args.get('stop_after_match'), matcher=matcher,
                    require_start_receipt='prepared_observations' in (invocation.flow_context or {}))
                summary = (f"Live tracker “{job['name']}” is {job['state']}. Checking every {job['interval_seconds']} seconds "
                           f"until {local_time(job['expires_at'])}. "
                           + ('Its alert condition was updated; its saved source is unchanged.' if matcher is not None
                              else 'Its saved source and condition are unchanged.'))
                output = {**public_tracker(job),'deferred_execution':_deferred_execution_receipt(job,duration),
                    'message':summary,'action_receipt':_action_receipt(action='updated',object_type='live_tracker',
                        object_id=job['id'],object_name=job['name'],summary=summary,delivery_required=True)}
            elif invocation.tool_name in {"stop_live_tracker", "resume_live_tracker"}:
                ident = args.get("id")
                if not isinstance(ident, str) or not ident:
                    raise ValueError(
                        "Choose a live tracker from list_live_trackers first"
                    )
                saved = store.get(ident, workspace)
                duration = args.get("duration_seconds")
                explicit_end = args.get('expires_at')
                if (invocation.tool_name == "resume_live_tracker" and saved
                        and (saved["state"] == "expired" or saved["expires_at"] <= utcnow().isoformat()) and duration is None and explicit_end is None):
                    offer = store.renewal_offer(ident, workspace, _conversation(invocation))
                    from nullion.live_tracking import timestamp
                    if (offer and offer["selected"] is None and offer["valid_until"] > timestamp(utcnow())
                            and offer["conversation_id"] == _conversation(invocation)):
                        question = (f"The previous window ended for “{saved['name']}”. How long should I continue with the same source and condition? Reply with a number:\n\n"
                                    "1. Continue for 1 more hour.\n2. Continue for 2 more hours.\n"
                                    "3. Continue for 4 more hours.\n4. Keep it stopped.")
                        extra = {"extension_choice_id": offer["id"]}
                    else:
                        question = DURATION_CHOICE
                        extra = {}
                    return ToolResult(invocation.invocation_id, invocation.tool_name, "failed", {
                        "reason": "live_tracking_needs_details", "clarification": question,
                        "tracker_id": ident, **extra,
                    }, "The previous window ended. Choose a new tracking duration.")
                job = (store.stop(ident, workspace) if invocation.tool_name == "stop_live_tracker"
                       else store.restart(ident, workspace, _conversation(invocation), duration,
                           (invocation.capsule_id or invocation.invocation_id) + ":restart:" + ident, expires_at=explicit_end)
                       if duration is not None or explicit_end is not None else store.resume(ident, workspace))
                if job is None:
                    raise ValueError("That live tracker is not in this workspace")
                summary = f"Live tracker “{job['name']}” is {job['state']}."
                if job["state"] == "active":
                    summary += (f" Checking every {job['interval_seconds']} seconds until {local_time(job['expires_at'])}. "
                                + ('Stopping after the first match. ' if job.get('stop_after_match', True)
                                   else 'I’ll keep checking and alert on new changes until the end time. ')
                                + "I’ll notify you when this window ends.")
                output = {
                    **public_tracker(job),
                    **({"deferred_execution": _deferred_execution_receipt(job, duration)} if job["state"] == "active" else {}),
                    "source_status": "pending" if not job.get("checks") else job.get("last_outcome"),
                    "message": summary,
                    "action_receipt": _action_receipt(
                        action="extended" if (duration is not None or explicit_end is not None) and invocation.tool_name == "resume_live_tracker" else job["state"],
                        object_type="live_tracker",
                        object_id=job["id"],
                        object_name=job["name"],
                        summary=summary,
                        delivery_required=True,
                    ),
                }
            else:
                if args.get("duration_seconds") is None and args.get('expires_at') is None:
                    return ToolResult(
                        invocation.invocation_id,
                        invocation.tool_name,
                        "failed",
                        {
                            "reason": "live_tracking_needs_details",
                            "clarification": DURATION_CHOICE,
                        },
                        "A bounded tracking window is required",
                    )
                duration = args.get("duration_seconds")
                try:
                    tracking_deadline(duration_seconds=duration, expires_at=args.get('expires_at'))
                except ValueError as exc:
                    return ToolResult(
                        invocation.invocation_id,
                        invocation.tool_name,
                        "failed",
                        {
                            "reason": "live_tracking_needs_details",
                            "clarification": DURATION_CHOICE,
                        },
                        str(exc),
                    )
                interval = args.get("interval_seconds", DEFAULT_INTERVAL)
                if type(interval) is not int or not MIN_INTERVAL <= interval <= MAX_INTERVAL:
                    return ToolResult(
                        invocation.invocation_id,
                        invocation.tool_name,
                        "failed",
                        {
                            "reason": "live_tracking_needs_details",
                            "clarification": f"How often should I check? Live tracking supports {MIN_INTERVAL} to {MAX_INTERVAL} seconds.\n\n"
                            "1. Every 15 seconds (default).\n2. Every 30 seconds.\n3. Every minute.\n"
                            "You can also choose another interval within that range.",
                        },
                        "A supported live check interval is required",
                    )
                try:
                    probe, matcher = validate_plan(args.get("probe"), args.get("matcher"), registry)
                except LiveTrackerPlanError:
                    raise
                except (ValueError, KeyError, TypeError) as exc:
                    # This phase precedes any mutation. Give every structural
                    # validation failure the same bounded argument-repair contract.
                    raise LiveTrackerPlanError(str(exc), code="invalid_observation_plan") from exc
                validate_prepared_observations(
                    probe,
                    matcher,
                    (invocation.flow_context or {}).get("prepared_observations"),
                    (invocation.flow_context or {}).get("observation_preparations"),
                )
                prepared = (invocation.flow_context or {}).get("prepared_observations")
                evaluator = getattr(registry, "live_tracker_model_client_getter", None)
                initial_snapshot = None
                if matcher["kind"] == "semantic" and prepared is not None and callable(evaluator):
                    outputs = [prepared[json.dumps(step, ensure_ascii=False, sort_keys=True, default=str)] for step in probe]
                    check = evaluate_semantic(matcher, outputs, evaluator())
                    if check.status not in {"matched", "no_match"}:
                        problem = observation_problem(check)
                        raise LiveTrackerPlanError("The prepared source cannot evaluate this condition: " + problem["message"] +
                                         " Repair and test a small dedicated READ source before creating this tracker. Include all required result fields, such as direct URLs.",
                                         code="semantic_condition_unresolved", problem=problem,
                                         observation_history_available="observation_history" in (check.diagnostics or {}))
                    if check.comparison_snapshot is not None:
                        from nullion.live_tracking import snapshot_plan_binding, _semantic_packet, MAX_SEMANTIC_PACKET_CHARS
                        # Admission must reserve room for a real prior snapshot,
                        # not just a small first-observation envelope.
                        if len(_semantic_packet(matcher, outputs, previous_observations=outputs)) > MAX_SEMANTIC_PACKET_CHARS:
                            raise LiveTrackerPlanError('Narrow the source so both current and previous observations fit losslessly.',
                                code='comparison_history_too_large')
                        initial_snapshot = dict(check.comparison_snapshot)
                # Carry only trusted runtime session state. Tool arguments and
                # user text cannot choose another conversation's browser handle.
                source_refresh = args.get("source_refresh", DEFAULT_SOURCE_REFRESH)
                if source_refresh not in {"reuse", "navigate"}:
                    raise LiveTrackerPlanError("Choose a registered source refresh mode", code="invalid_source_refresh", allowed=["reuse", "navigate"])
                context = invocation.flow_context or {}
                prepared = context.get("prepared_observations") or {}
                probe_contexts = []
                for step in probe:
                    source_context = {
                        key: context[key]
                        for key in ("browser_session_scope", "browser_session_owner", "browser_session_id")
                        if isinstance(context.get(key), str) and context[key]
                    }
                    signature = json.dumps(step, ensure_ascii=False, sort_keys=True, default=str)
                    observed = prepared.get(signature)
                    schema = registry.get_spec(step["tool_name"]).input_schema or {}
                    if (source_context and "session_id" in schema.get("properties", {})
                            and isinstance(observed, dict) and isinstance(observed.get("session_id"), str)
                            and observed["session_id"]):
                        source_context["browser_session_id"] = observed["session_id"]
                    from nullion.observation_replay import validated_observation_setup
                    setup = validated_observation_setup(registry, step["tool_name"], observed)
                    if setup:
                        # A creator's physical tab is process-local. Bind the
                        # recipe, not the creator's handle, to the worker.
                        source_context = {"observation_setup": setup, "observation_refresh": source_refresh}
                    probe_contexts.append(source_context)
                now = utcnow()
                if initial_snapshot is not None:
                    initial_snapshot['plan_binding'] = snapshot_plan_binding(probe, probe_contexts)
                job = store.create(
                    name=args.get("name"),
                    principal_id=invocation.principal_id,
                    conversation_id=_conversation(invocation),
                    workspace_id=workspace,
                    probe=probe,
                    probe_contexts=probe_contexts,
                    matcher=matcher,
                    interval_seconds=args.get("interval_seconds", DEFAULT_INTERVAL),
                    expires_at=tracking_deadline(duration_seconds=duration, expires_at=args.get('expires_at'),now=now),
                    request_key=(invocation.capsule_id or invocation.invocation_id)
                    + ":"
                    + hashlib.sha256(str(args.get("name", "")).encode()).hexdigest()[
                        :16
                    ],
                    notify_on_expiry=args.get("notify_on_expiry", True),
                    require_start_receipt="prepared_observations"
                    in (invocation.flow_context or {}),
                    initial_comparison_snapshot=initial_snapshot,
                    stop_after_match=args.get('stop_after_match', False),
                    now=now,
                )
                summary = (
                    f"Live tracker “{job['name']}” is {job['state']}. Checking every {job['interval_seconds']} seconds "
                    f"until {local_time(job['expires_at'])}. "
                    + ('Stopping after the first match.' if job.get('stop_after_match', True)
                       else 'I’ll keep checking for new changes until the end time.')
                ) if job['state'] == 'active' else f"Live tracker “{job['name']}” is {job['state']}. No checks are running."
                output = {
                    **public_tracker(job),
                    **({"deferred_execution": _deferred_execution_receipt(job, duration, job["created_at"])}
                       if job['state'] == 'active' else {}),
                    "message": summary,
                    "action_receipt": _action_receipt(
                        action="created" if job['state'] == 'active' else 'unchanged',
                        object_type="live_tracker",
                        object_id=job["id"],
                        object_name=job["name"],
                        summary=summary,
                        delivery_required=True,
                        details=["I’ll alert you in this chat."] if job['state'] == 'active' else [],
                    ),
                }
            wake = getattr(registry, "live_tracker_wake", None)
            if callable(wake):
                wake()
            return ToolResult(
                invocation.invocation_id, invocation.tool_name, "completed", output
            )
        except (ValueError, KeyError, TypeError) as exc:
            if isinstance(exc, TrackingDeadlineError):
                return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
                    {'reason': 'live_tracking_needs_details', 'clarification': str(exc)+'\n\n'+DURATION_CHOICE}, str(exc))
            context = invocation.flow_context or {}
            ready = [receipt["probe"] for receipt in (context.get("observation_preparations") or {}).values()
                     if receipt.get("status") == "ready"]
            return ToolResult(
                invocation.invocation_id,
                invocation.tool_name,
                "failed",
                {"reason": "live_tracker_invalid_plan",
                 **({"reason": "invalid_tool_arguments", "validation_kind": "live_tracker_invalid_plan",
                     "executed": False, "validation": exc.validation, "ready_prepared_probes": ready,
                     "recovery": {"kind": "correct_arguments", "retry_tool_name": invocation.tool_name,
                         "argument_constraints": {"validation": exc.validation,
                             "ready_prepared_probes": ready, "requires_verified_probe": True}}}
                    if isinstance(exc, LiveTrackerPlanError) else {})},
                str(exc),
            )

    schemas = {
        "create_live_tracker": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "maxLength": 160},
                "duration_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Full user-selected lifetime from tool acceptance, in seconds. Pass the requested duration unchanged; do not subtract setup time. No product maximum. For an exact wall-clock deadline use expires_at instead; ask when neither is known.",
                },
                "expires_at": {"type":"string","format":"date-time","description":"Explicit user-selected future deadline with time zone. Use instead of duration_seconds, never together. Setup time does not move this exact clock deadline."},
                "interval_seconds": {
                    "type": "integer",
                    "minimum": MIN_INTERVAL,
                    "maximum": MAX_INTERVAL,
                    "default": DEFAULT_INTERVAL,
                },
                "notify_on_expiry": {"type": "boolean", "default": True},
                "stop_after_match": {"type":"boolean","default":False,
                    "description":"Default false: keep checking for new changes until the mandatory end time, without continuation prompts after alerts. Use true only for an explicitly requested one-shot alert."},
                "source_refresh": {"type": "string", "enum": ["reuse", "navigate"], "default": DEFAULT_SOURCE_REFRESH,
                    "description": "Default navigate refreshes the saved source in the same worker tab for each check; it does not create another tab or restart the assistant. Choose reuse only for a verified self-updating page whose DOM stays current without navigation. Startup replay rebuilds a missing worker tab; it does not replace the saved polling plan."},
                "probe": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {
                            "tool_name": {"type": "string"},
                            "arguments": {"type": "object"},
                        },
                        "required": ["tool_name", "arguments"],
                    },
                    "description": "Prepared dedicated READ data tools only, without generating artifacts or screenshots. Test the source first. Include refresh/navigation when needed; never rely on an unrelated open page. No shell, writes, agent-run tools or generic gateways.",
                },
                "matcher": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": ["structured", "semantic"]},
                        "conditions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "step": {"type": "integer", "minimum": 0,
                                             "description": "Zero-based index into probe. With one probe step, use 0."},
                                    "pointer": {"type": "string"},
                                    "label": {
                                        "type": "string",
                                        "maxLength": 60,
                                        "description": "Human label for the observed matching value in the alert.",
                                    },
                                    "op": {
                                        "type": "string",
                                        "enum": list(STRUCTURED_OPERATORS),
                                    },
                                    "value": {},
                                },
                                "required": ["step", "pointer", "op"],
                            },
                        },
                        "summary": {
                            "type": "string",
                            "description": "Concise alert text for a structured match.",
                        },
                        "instruction": {
                            "type": "string",
                            "description": "For semantic mode only: exact condition, target and freshness requirements. A small bounded evidence check runs per poll; no task planning. For newly appearing collection records, use a source declaring change_identity_fields: durable observation_history then identifies baseline, seen and new records across polls and restarts. For other deterministic changes prefer structured changed. For section-based conditions, preserve the section heading and scope the read to that region. Treat section membership as the label; do not require the heading to repeat in each item. Verify coverage before starting.",
                        },
                        "history_mode": {
                            "type": "string", "enum": ["records", "snapshot"],
                            "description": "Use snapshot for changes to existing values or page content: the previous successful observation is stored across polls and restarts. Use records (default) for newly appearing items from a source declaring stable change_identity_fields. Choose this by the requested condition, not the source vendor. Use a complete scoped content read, never a clipped locator preview.",
                        },
                    },
                    "required": ["kind"],
                    "anyOf": [
                        {"properties": {"kind": {"enum": ["structured"]}}, "required": ["conditions", "summary"]},
                        {"properties": {"kind": {"enum": ["semantic"]}}, "required": ["instruction"]},
                    ],
                    "description": "Prefer structured conditions over verified source output JSON pointers, relative to the output object itself (for example /field, without an /output wrapper). Missing fields are unknown, never a match. Do not use exists for a field that contains unavailable/placeholder states; exclude those observed states explicitly. changed establishes its baseline on the first check. Use semantic only when deterministic comparisons cannot express the observed condition.",
                },
            },
            "required": ["name", "probe", "matcher"],
        },
        "list_live_trackers": {
            "type": "object",
            "properties": {
                "include_finished": {"type": "boolean", "default": True},
                "query": {
                    "type": "string",
                    "description": "Optional tracker name or id filter.",
                },
            },
        },
        "stop_live_tracker": {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
        },
        "resume_live_tracker": {
            "type": "object",
            "properties": {"id": {"type": "string"}, "duration_seconds": {
                "type": "integer", "minimum": 1,
                "description": "Explicit user-selected length of a NEW window. Required for an expired tracker; omit to preserve an unexpired deadline. Never invent a duration.",
            }, "expires_at":{"type":"string","format":"date-time","description":"Explicit future end time with time zone, instead of duration_seconds."}},
            "required": ["id"],
        },
        "update_live_tracker": {"type":"object","properties":{
            "id":{"type":"string"},"duration_seconds":{"type":"integer","minimum":1,
                "description":"Full requested duration from this update's acceptance; do not subtract prior setup time."},
            "expires_at":{"type":"string","format":"date-time","description":"Exact future deadline with time zone, instead of duration_seconds."},
            "interval_seconds":{"type":"integer","minimum":MIN_INTERVAL,"maximum":MAX_INTERVAL},
            "stop_after_match":{"type":"boolean","description":"False keeps monitoring new changes until the saved deadline; true explicitly requests a one-shot alert."}},"required":["id"]},
    }
    # Both mutations use the same validated condition schema, not separate
    # vendor-specific filter parameters or natural-language routing rules.
    schemas['update_live_tracker']['properties']['matcher'] = {
        **schemas['create_live_tracker']['properties']['matcher'],
        'description': 'Complete replacement alert condition. Preserve saved history_mode unless the user changes comparison semantics. Omit to retain the condition. Never change the deadline or cadence unless requested.'}
    descriptions = {
        "create_live_tracker": (
            "Start a live tracker with a finite end time in the current chat. Reuses a small read-only source check every 15 seconds by default, "
            "independently of cron workers. Requires an explicit finite duration/deadline, with no product lifetime maximum. By default it keeps checking for new changes until that deadline, without asking to continue after each alert. "
            "Only use stop_after_match=true when the user explicitly requests a one-shot alert. "
            "Use for condition-based monitoring within any chosen finite window; use create_cron for recurring scheduled work. If the monitoring condition, lifetime or recurring intent is unclear, "
            "ask the user to choose numbered live-versus-recurring options before creating anything. Prepare and test a narrow source probe first. "
            "When the user explicitly requests creation and supplies the lifetime and check frequency, that instruction authorizes creation "
            "subject to the current permission policy. Verify the source and create the tracker; do not add a redundant confirmation question. "
            "To amend active timing use update_live_tracker; never stop and recreate a valid saved source merely to correct its duration or cadence. "
            "For continuation of an existing saved tracker, use list_live_trackers and resume_live_tracker with its id and a user-selected new duration or expires_at; "
            "reuse its stored probe instead of creating a duplicate or preparing the source again. Request that scope when needed. "
            "Never claim a live tracker was created if its result is a clarification or failure."
        ),
        "list_live_trackers": "List temporary live trackers, their deadlines, check frequency, current state and last check in this workspace. Distinct from recurring crons.",
        "stop_live_tracker": "Stop the exact temporary live tracker identified by current conversation context or list_live_trackers. Does not delete recurring crons.",
        "update_live_tracker": "Amend an active or paused live tracker's alert condition (matcher), end time, cadence, or stop_after_match policy in its original chat. Read the saved matcher with list_live_trackers, then pass the complete revised matcher to change which updates qualify. Reuses its id, source and observation history; does not restart or extend the window for a condition-only update. A paused tracker stays paused. Set stop_after_match=false to keep alerting on new changes until the deadline, without per-alert continuation choices. Use duration_seconds for the full requested new window from acceptance, or expires_at for an exact future deadline; never both. Omitted fields keep their saved values. For finished trackers use resume_live_tracker.",
        "resume_live_tracker": "Continue a matched tracker for new results, or resume a blocked or stopped tracker after its source/access issue was resolved. Reuse the saved tracker rather than creating a duplicate. Previously found results are skipped. Without duration_seconds, preserves an unexpired deadline. For an expired tracker, reuse the saved setup with an explicit user-selected duration_seconds. If no duration is known, call this tool without it to offer numbered continuation choices; do not repeat the identical failed call.",
    }
    for name, schema in schemas.items():
        try:
            registry.get_spec(name)
        except KeyError:
            pass
        else:
            continue
        registry.register(
            ToolSpec(
                name=name,
                description=descriptions[name],
                risk_level=ToolRiskLevel.LOW,
                side_effect_class=(
                    ToolSideEffectClass.READ
                    if name == "list_live_trackers"
                    else ToolSideEffectClass.WRITE
                ),
                requires_approval=False,
                timeout_seconds=EVALUATION_TIMEOUT_SECONDS + 5 if name == "create_live_tracker" else 10,
                action_review_context=creation_review_context if name == 'create_live_tracker' else None,
                capability_tags=("live_tracking", "scheduler_inventory") if name == 'list_live_trackers' else ("live_tracking",),
                replay_safe_after_completion=name != "list_live_trackers",
                input_schema=schema,
                discovery_parameters=tuple(key for key in ("interval_seconds", "duration_seconds", "expires_at")
                                           if key in schema.get("properties", {})),
                continuation_tools=(
                    ("stop_live_tracker", "resume_live_tracker", "update_live_tracker")
                    if name == "list_live_trackers"
                    else ("list_live_trackers", "update_live_tracker") if name == "create_live_tracker"
                    else ("list_live_trackers", "resume_live_tracker") if name == "update_live_tracker" else ()
                ),
            ),
            invoke,
        )
    return registry
