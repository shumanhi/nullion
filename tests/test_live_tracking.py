"""Contracts for bounded observation plans and transactional live tracking."""

from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from nullion.live_tracking import (
    LiveTrackerStore,
    LiveTrackerEngine,
    Observation,
    evaluate_structured,
    evaluate_semantic,
    validate_plan,
    public_tracker,
    utcnow,
    MAX_INTERVAL,
)
from nullion.tools import (
    ToolRegistry,
    ToolSpec,
    ToolRiskLevel,
    ToolSideEffectClass,
    ToolResult,
    ToolInvocation,
)


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    registry = ToolRegistry()
    readings = []

    def read(inv):
        readings.append(inv)
        return ToolResult(
            inv.invocation_id, inv.tool_name, "completed", {"reading": 23}
        )

    registry.register(
        ToolSpec(
            "sensor_read",
            "Read sensor",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            2,
        ),
        read,
    )
    store = LiveTrackerStore(tmp_path / "runtime.db")
    return SimpleNamespace(
        runtime=runtime, registry=registry, store=store, readings=readings
    )


def matcher(op="greater_than", value=20):
    return {
        "kind": "structured",
        "conditions": [
            {
                "step": 0,
                "pointer": "/reading",
                "op": op,
                "value": value,
                "label": "Reading",
            }
        ],
        "summary": "The sensor condition is met.",
    }




def create(f, *, now=None, **overrides):
    now = now or utcnow()
    kwargs = dict(
        name="Sensor watch",
        principal_id="web:alpha",
        conversation_id="web:alpha",
        workspace_id="workspace_admin",
        probe=[{"tool_name": "sensor_read", "arguments": {}}],
        matcher=matcher(),
        interval_seconds=5,
        expires_at=now + timedelta(seconds=90),
        request_key="create-a",
        now=now,
    )
    kwargs.update(overrides)
    return f.store.create(**kwargs)


@pytest.mark.parametrize(
    "op,value,reading,expected",
    [
        ("equals", 23, 23, "matched"),
        ("equals", False, 0, "no_match"),
        ("not_equals", 23, 23, "no_match"),
        ("greater_than", 20, 23, "matched"),
        ("less_than", 20, 23, "no_match"),
        ("in", [20, 23], 23, "matched"),
        ("not_in", [None, ""], 23, "matched"),
        ("exists", None, None, "no_match"),
        ("exists", None, 0, "matched"),
        ("greater_than", 20, "23", "unknown"),
        ("not_equals", None, "pending", "matched"),
    ],
)
def test_structured_comparisons_are_typed(op, value, reading, expected):
    result = evaluate_structured(matcher(op, value), [{"reading": reading}], None)
    assert result.status == expected
    if expected == "matched" and reading is not None:
        assert str(reading) in result.summary


def test_missing_field_is_unknown_and_changed_records_initial_baseline():
    assert (
        evaluate_structured(matcher("not_equals", "pending"), [{}], None).status
        == "unknown"
    )
    assert evaluate_structured(matcher("exists"), [{}], None).status == "unknown"
    first = evaluate_structured(matcher("changed"), [{"reading": 12}], None)
    assert first.status == "no_match" and first.baseline == [12]
    changed = evaluate_structured(matcher("changed"), [{"reading": 13}], first.baseline)
    assert changed.status == "matched"


@pytest.mark.parametrize(
    "side,delegated",
    [
        (ToolSideEffectClass.WRITE, ()),
        (ToolSideEffectClass.DANGEROUS_EXEC, ()),
        (ToolSideEffectClass.READ, ("other",)),
    ],
)
def test_probe_cannot_repeat_writes_execution_or_delegated_gateways(
    fixture, side, delegated
):
    fixture.registry.register(
        ToolSpec(
            "unsafe",
            "Other tool",
            ToolRiskLevel.HIGH,
            side,
            True,
            2,
            delegated_tools=delegated,
        ),
        lambda inv: None,
    )
    with pytest.raises(ValueError, match="read-only"):
        validate_plan(
            [{"tool_name": "unsafe", "arguments": {}}], matcher(), fixture.registry
        )


def test_store_idempotency_workspace_boundary_and_checkpoint_restart(fixture):
    a = create(fixture)
    duplicate = create(fixture, name="Different retry name")
    assert duplicate["id"] == a["id"]
    assert fixture.store.get(a["id"], "workspace_other") is None
    assert fixture.store.stop(a["id"], "workspace_other") is None
    assert fixture.store.list("workspace_other") == []
    fixture.runtime.checkpoint(force=True)
    reopened = LiveTrackerStore(fixture.store.path)
    assert reopened.get(a["id"])["probe"] == a["probe"]
    assert "probe" not in public_tracker(a) and "principal_id" not in public_tracker(a)


@pytest.mark.parametrize(
    "field,value",
    [
        ("interval_seconds", True),
        ("interval_seconds", 1),
        ("interval_seconds", MAX_INTERVAL + 1),
        ("notify_on_expiry", "yes"),
        ("expires_at", datetime.now(timezone.utc) - timedelta(seconds=1)),
    ],
)
def test_invalid_windows_and_cadence_are_rejected(fixture, field, value):
    with pytest.raises(ValueError):
        create(fixture, **{field: value})


def test_capacity_leases_survive_worker_restart_and_only_one_result_can_finish(fixture):
    a = create(fixture)
    create(fixture, request_key="b")
    job, token = fixture.store.claim(1)
    restarted = LiveTrackerStore(fixture.store.path)
    assert restarted.claim(1) is None
    assert (
        restarted.finish(job["id"], "wrong-token", Observation("matched", "Wrong"))
        is False
    )
    assert (
        restarted.finish(job["id"], token, Observation("matched", "Reading 23")) is True
    )
    assert (
        restarted.finish(job["id"], token, Observation("matched", "Duplicate")) is False
    )
    one = restarted.claim_notice()
    assert one and restarted.claim_notice() is None
    restarted.finish_notice(one, True)
    assert restarted.claim_notice() is None


def test_stop_during_check_suppresses_late_match_and_can_resume_before_deadline(
    fixture,
):
    a = create(fixture)
    job, token = fixture.store.claim(4)
    fixture.store.stop(a["id"], "workspace_admin")
    assert not fixture.store.finish(
        a["id"], token, Observation("matched", "Late result")
    )
    assert fixture.store.claim_notice() is None
    resumed = fixture.store.resume(a["id"], "workspace_admin")
    assert resumed["expires_at"] == a["expires_at"] and resumed["state"] == "active"


def test_expiry_never_delivers_late_match_or_resumes_expired_job(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    job, token = fixture.store.claim(1, now)
    future = now + timedelta(seconds=91)
    assert not fixture.store.finish(
        a["id"], token, Observation("matched", "Late"), future
    )
    fixture.store.expire(future)
    assert fixture.store.get(a["id"])["state"] == "expired"
    notice = fixture.store.claim_notice(future)
    assert (
        notice["kind"] == "expired"
        and "No matching result was confirmed" in notice["text"]
    )
    fixture.store.finish_notice(notice, True, future)
    fixture.store.expire(future)
    assert fixture.store.claim_notice(future) is None
    with pytest.raises(ValueError):
        fixture.store.resume(a["id"], "workspace_admin", future)


def test_three_unverified_checks_pause_without_claiming_no_match(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    for index in range(3):
        now += timedelta(seconds=20)
        job, token = fixture.store.claim(4, now)
        assert fixture.store.finish(a["id"], token, Observation("unknown"), now)
    job = fixture.store.get(a["id"])
    assert job["state"] == "blocked" and job["checks"] == 3
    notice = fixture.store.claim_notice(now)
    assert (
        "The last three checks" in notice["text"]
        and "Reason**" in notice["text"] and "How to fix it**" in notice["text"]
        and "Tracking is paused" in notice["text"]
        and notice["kind"] == "blocked"
    )
    fixture.store.expire(datetime.fromisoformat(a["expires_at"]) + timedelta(seconds=1))
    fixture.store.finish_notice(notice, True, now)
    assert fixture.store.claim_notice(now) is None


def test_failed_delivery_retries_same_notice_until_deadline_then_stops(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    job, token = fixture.store.claim(4, now)
    fixture.store.finish(a["id"], token, Observation("matched", "Reading 23"), now)
    notice = fixture.store.claim_notice(now)
    fixture.store.finish_notice(notice, False, now)
    assert fixture.store.claim_notice(now) is None
    again = fixture.store.claim_notice(now + timedelta(seconds=16))
    assert again["id"] == notice["id"]
    assert fixture.store.claim_notice(now + timedelta(seconds=61)) is None


def test_engine_reuses_read_plan_without_agent_or_model_calls(fixture, monkeypatch):
    import time

    f = fixture
    now = utcnow()
    a = create(f, now=now, matcher=matcher("equals", 99))

    class Model:
        def create(self, **kw):
            time.sleep(0.4)
            pytest.fail("Structured live polling must never call the model")

    engine = LiveTrackerEngine(
        f.store,
        registry=f.registry,
        runtime_store=f.runtime.store,
        deliver=lambda *a: True,
        model_client=Model(),
    )
    elapsed = []
    for index in range(4):
        now += timedelta(seconds=6)
        monkeypatch.setattr("nullion.live_tracking.utcnow", lambda: now)
        job, token = f.store.claim(4, now)
        start = time.perf_counter()
        engine.check_once(job, token)
        elapsed.append(time.perf_counter() - start)
    assert len(f.readings) == 4 and f.store.get(a["id"])["checks"] == 4
    assert all(inv.flow_context["flow_kind"] == "live_tracker" for inv in f.readings)
    assert sum(elapsed) / len(elapsed) < 0.2 and max(elapsed) < 0.35, elapsed


@pytest.mark.parametrize(
    "payload",
    [
        {
            "status": "matched",
            "summary": "Wrong",
            "evidence": [{"step": 0, "pointer": "/missing", "value": 23}],
        },
        {
            "status": "matched",
            "summary": "Wrong",
            "evidence": [{"step": 0, "pointer": "/reading", "value": 99}],
        },
        {"status": "matched", "summary": "Wrong", "evidence": []},
    ],
)
def test_semantic_match_requires_exact_current_tool_evidence(payload):
    model = SimpleNamespace(
        create=lambda **kw: {"content": [{"type": "text", "text": json.dumps(payload)}]}
    )
    assert (
        evaluate_semantic(
            {"kind": "semantic", "instruction": "Watch the requested condition"},
            [{"reading": 23}],
            model,
        ).status
        == "unknown"
    )


def test_tracking_scope_choices_are_typed_and_do_not_expand_read_discovery(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.turn_context_policy import (
        ScopedTurnToolRegistry,
        TurnToolEvidence,
        TurnToolScopeDecision,
    )

    register_live_tracker_tools(fixture.registry)
    fixture.registry.register(
        ToolSpec(
            "create_cron",
            "Recurring work",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.WRITE,
            False,
            2,
        ),
        lambda inv: None,
    )
    scoped = ScopedTurnToolRegistry(fixture.registry, evidence=TurnToolEvidence())
    _, read_scope = scoped.apply_scope_request(
        ToolInvocation(
            "read",
            "request_tool_scope",
            "web:alpha",
            {"capabilities": ["live_tracking"]},
        )
    )
    assert read_scope.can_invoke_tool("list_live_trackers")
    assert not read_scope.can_invoke_tool("stop_live_tracker")
    _, live = scoped.apply_scope_request(
        ToolInvocation(
            "live",
            "request_tool_scope",
            "web:alpha",
            {
                "capabilities": ["live_tracking"],
                "tool_names": ["create_live_tracker"],
                "tracking_mode": "live",
            },
        )
    )
    assert live.can_invoke_tool("create_live_tracker") and not live.can_invoke_tool(
        "create_cron"
    )
    _, clarify = scoped.apply_scope_request(
        ToolInvocation(
            "clarify",
            "request_tool_scope",
            "web:alpha",
            {"capabilities": ["live_tracking"], "tracking_mode": "clarify"},
        )
    )
    assert not clarify.can_invoke_tool(
        "create_live_tracker"
    ) and not clarify.can_invoke_tool("create_cron")


def test_delivery_expiry_visible_and_pruning_preserves_active(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    job, token = fixture.store.claim(4, now)
    fixture.store.finish(a["id"], token, Observation("matched", "Reading 23"), now)
    assert fixture.store.claim_notice(now + timedelta(seconds=61)) is None
    assert fixture.store.get(a["id"])["notification_status"] == "undelivered"
    fixture.store.prune(now + timedelta(days=8))
    assert fixture.store.get(a["id"]) is None
    current = create(fixture, request_key="new")
    assert (
        fixture.store.list("workspace_admin", query="SENSOR")[0]["id"] == current["id"]
    )
    assert fixture.store.list("workspace_admin", query="not present") == []


@pytest.mark.parametrize("capacity", [0, 17, True, "2", None])
def test_invalid_parallel_settings(fixture, capacity):
    with pytest.raises(ValueError):
        fixture.store.set_capacity(capacity)


def test_released_or_expired_claim_is_recoverable(fixture):
    now = utcnow()
    a = create(fixture, now=now, expires_at=now + timedelta(seconds=300))
    job, token = fixture.store.claim(1, now)
    fixture.store.release_claim(a["id"], "wrong")
    assert fixture.store.claim(1, now) is None
    fixture.store.release_claim(a["id"], token)
    job, new = fixture.store.claim(1, now)
    assert new != token
    restarted = LiveTrackerStore(fixture.store.path)
    assert restarted.claim(1, now + timedelta(seconds=121))
    assert not fixture.store.finish(a["id"], new, Observation("matched", "Stale"), now)


def test_thread_start_failure_releases_capacity(fixture, monkeypatch):
    a = create(fixture)
    engine = LiveTrackerEngine(
        fixture.store,
        registry=fixture.registry,
        runtime_store=fixture.runtime.store,
        deliver=lambda *a: True,
    )
    monkeypatch.setattr(
        "threading.Thread.start",
        lambda self: (_ for _ in ()).throw(RuntimeError("No worker")),
    )
    engine.tick()
    assert engine._running == {} and fixture.store.claim(1) is not None


def test_required_action_receipt_is_presented_without_extra_model_call():
    from nullion.response_sanitizer import sanitize_user_visible_reply
    from nullion.tools import _action_receipt

    receipt = _action_receipt(
        action="created",
        object_type="observation",
        object_id="a",
        object_name="Sensor",
        summary="Sensor watch is active, checking every 15 seconds for 1 hour.",
        delivery_required=True,
    )
    result = ToolResult(
        "x", "observation_start", "completed", {"action_receipt": receipt}
    )
    for draft in ["Processed.", "完了しました。", "Listo."]:
        actual = sanitize_user_visible_reply(
            user_message="Observe this", reply=draft, tool_results=[result]
        )
        assert receipt["summary"] in actual
    browser = ToolResult(
        "browser", "browser_navigate", "completed", {"url": "https://example.com"}
    )
    assert receipt["summary"] in sanitize_user_visible_reply(
        user_message="Observe", reply="Done.", tool_results=[browser, result]
    )
    from nullion.response_sanitizer import _required_action_receipt_reply

    assert (
        _required_action_receipt_reply(
            "Could not stop.",
            [result, ToolResult("stop", "observation_stop", "failed", {})],
        )
        is None
    )


def test_source_access_pauses_and_resume_keeps_original_deadline(fixture):
    a = create(fixture)
    job, token = fixture.store.claim(4)
    fixture.store.finish(a["id"], token, Observation("source_access"))
    notice = fixture.store.claim_notice()
    assert "needs approval" in notice["text"]
    resumed = fixture.store.resume(a["id"], "workspace_admin")
    assert resumed["expires_at"] == a[
        "expires_at"
    ] and not fixture.store.notice_is_current(notice)


def test_oversize_semantic_observation_does_not_call_model():
    model = SimpleNamespace(
        create=lambda **kw: pytest.fail("Do not truncate evidence JSON")
    )
    assert (
        evaluate_semantic(
            {"kind": "semantic", "instruction": "Observe"},
            [{"body": "x" * 60000}],
            model,
        ).status
        == "unknown"
    )


def test_prepared_condition_must_bind_to_exact_source_and_output_shape():
    from nullion.live_tracking import validate_prepared_observations

    plan = [{"tool_name": "sensor_read", "arguments": {"target": "birch"}}]
    signature = json.dumps(plan[0], ensure_ascii=False, sort_keys=True)
    observations = {signature: {"reading": 23}}
    validate_prepared_observations(plan, matcher(), observations)
    wrong = matcher()
    wrong["conditions"][0]["pointer"] = "/output/reading"
    with pytest.raises(ValueError, match="pointer"):
        validate_prepared_observations(plan, wrong, observations)
    with pytest.raises(ValueError, match="exact probe"):
        validate_prepared_observations(plan, matcher(), {})


@pytest.mark.parametrize("timeout", [0, -1, 31, float("nan")])
def test_unbounded_or_slow_sources_cannot_be_live_probes(fixture, timeout):
    fixture.registry.register(
        ToolSpec(
            "unbounded",
            "Source",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            timeout,
        ),
        lambda inv: None,
    )
    with pytest.raises(ValueError):
        validate_plan(
            [{"tool_name": "unbounded", "arguments": {}}], matcher(), fixture.registry
        )


def test_explicit_live_plan_replaces_prior_recurring_scope(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools
    from nullion.turn_context_policy import (
        ScopedTurnToolRegistry,
        TurnToolScopeDecision,
        TurnToolEvidence,
    )

    register_live_tracker_tools(fixture.registry)
    scoped = ScopedTurnToolRegistry(
        fixture.registry,
        evidence=TurnToolEvidence(context_linked=True),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="mutate",
            scheduler_effect_actions=("mutate",),
            requested_tool_names=("create_cron",),
            required_tool_names=("create_cron",),
            valid=True,
        ),
    )
    _, live = scoped.apply_scope_request(
        ToolInvocation(
            "switch",
            "request_tool_scope",
            "web:alpha",
            {
                "capabilities": ["live_tracking"],
                "tracking_mode": "live",
                "tool_names": ["create_live_tracker", "sensor_read"],
            },
        )
    )
    assert live.can_invoke_tool("create_live_tracker") and live.can_invoke_tool(
        "sensor_read"
    )
    assert live.turn_tool_scope_decision.scheduler_action == "none"
    assert "create_cron" not in live.turn_tool_scope_decision.required_tool_names


def test_distinct_named_trackers_in_one_structured_request_do_not_collapse(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools

    register_live_tracker_tools(fixture.registry)
    fixture.registry.live_tracker_store_path = fixture.store.path
    args = {
        "name": "Birch",
        "duration_seconds": 90,
        "interval_seconds": 5,
        "probe": [{"tool_name": "sensor_read", "arguments": {}}],
        "matcher": matcher(),
    }
    first = fixture.registry.invoke(
        ToolInvocation(
            "one", "create_live_tracker", "web:alpha", args, capsule_id="request-one"
        )
    )
    second = fixture.registry.invoke(
        ToolInvocation(
            "two",
            "create_live_tracker",
            "web:alpha",
            {**args, "name": "Willow"},
            capsule_id="request-one",
        )
    )
    retry = fixture.registry.invoke(
        ToolInvocation(
            "retry", "create_live_tracker", "web:alpha", args, capsule_id="request-one"
        )
    )
    assert first.status == second.status == retry.status == "completed"
    assert first.output["id"] == retry.output["id"] != second.output["id"]
    assert len(fixture.store.list("workspace_admin")) == 2


def test_live_probe_resolves_current_runtime_after_chat_refresh(fixture):
    from nullion.runtime_store import RuntimeStore

    f = fixture
    a = create(f)
    old = f.runtime.store
    engine = LiveTrackerEngine(
        f.store,
        registry=f.registry,
        runtime_store=lambda: f.runtime.store,
        deliver=lambda *a: True,
    )
    f.runtime.store = RuntimeStore()
    job, token = f.store.claim(4)
    engine.check_once(job, token)
    assert f.store.get(a["id"])["state"] == "matched"
    assert any(
        e.event_type == "tool.invocation.completed"
        for e in f.runtime.store.list_events()
    )
    assert not any(
        e.event_type == "tool.invocation.completed" for e in old.list_events()
    )


@pytest.mark.parametrize(
    "field,value", [("duration_seconds", 0), ("interval_seconds", MAX_INTERVAL + 1)]
)
def test_unsupported_live_options_ask_numbered_questions_without_starting(
    fixture, field, value
):
    from nullion.live_tracking_tools import register_live_tracker_tools

    register_live_tracker_tools(fixture.registry)
    fixture.registry.live_tracker_store_path = fixture.store.path
    args = {
        "name": "Birch",
        "duration_seconds": 90,
        "interval_seconds": 5,
        "probe": [{"tool_name": "sensor_read", "arguments": {}}],
        "matcher": matcher(),
        field: value,
    }
    result = fixture.registry.invoke(
        ToolInvocation("invalid", "create_live_tracker", "web:alpha", args)
    )
    assert (
        result.status == "failed"
        and result.output["reason"] == "live_tracking_needs_details"
    )
    assert (
        "1." in result.output["clarification"]
        and "2." in result.output["clarification"]
    )
    assert fixture.store.list("workspace_admin") == []


def test_crashed_worker_is_reclaimed_quickly_and_heartbeat_keeps_slow_reads_owned(
    fixture,
):
    from nullion.live_tracking import LEASE_SECONDS

    assert LEASE_SECONDS <= 15
    now = utcnow()
    a = create(fixture, now=now)
    job, token = fixture.store.claim(1, now)
    assert fixture.store.renew(a["id"], token, now + timedelta(seconds=10))
    assert fixture.store.claim(1, now + timedelta(seconds=16)) is None
    reclaimed = fixture.store.claim(1, now + timedelta(seconds=26))
    assert reclaimed and reclaimed[1] != token
    assert not fixture.store.finish(
        a["id"],
        token,
        Observation("matched", "Late old worker"),
        now + timedelta(seconds=26),
    )


def test_worker_start_is_idempotent_and_stop_can_be_followed_by_restart(fixture):
    engine = LiveTrackerEngine(
        fixture.store,
        registry=fixture.registry,
        runtime_store=fixture.runtime.store,
        deliver=lambda *a: True,
    )
    engine.start()
    first = engine._thread
    engine.start()
    assert engine._thread is first
    engine.stop()
    assert not first.is_alive()
    try:
        engine.start()
        assert engine._thread is not first and engine._thread.is_alive()
    finally:
        engine.stop()


def test_observation_plan_never_repeats_artifact_producers_disguised_as_reads(fixture):
    fixture.registry.register(
        ToolSpec(
            "capture_read",
            "Image capture",
            ToolRiskLevel.LOW,
            ToolSideEffectClass.READ,
            False,
            2,
            source_artifact_extensions=(".png",),
        ),
        lambda inv: None,
    )
    with pytest.raises(ValueError):
        validate_plan(
            [{"tool_name": "capture_read", "arguments": {}}],
            matcher(),
            fixture.registry,
        )


def test_all_required_receipts_are_visible_and_newer_state_supersedes_creation():
    from nullion.tools import _action_receipt
    from nullion.response_sanitizer import _required_action_receipt_reply

    def result(ident, name, summary, required=True):
        return ToolResult(
            ident,
            "action",
            "completed",
            {
                "action_receipt": _action_receipt(
                    action="updated",
                    object_type="observation",
                    object_id=ident,
                    object_name=name,
                    summary=summary,
                    delivery_required=required,
                )
            },
        )

    first = result("a", "Birch", "Birch is active.")
    second = result("b", "Willow", "Willow is active.")
    assert (
        _required_action_receipt_reply("完了しました。", [first, second])
        == "完了しました。\n\nBirch is active.\n\nWillow is active."
    )
    stopped = result("a", "Birch", "Birch is stopped.")
    assert (
        _required_action_receipt_reply("Done.", [first, second, stopped])
        == "Done.\n\nWillow is active.\n\nBirch is stopped."
    )
    other = result("c", "Report", "Report saved.", False)
    rendered = _required_action_receipt_reply("The report is attached.", [first, other])
    assert "The report is attached." in rendered and "Birch is active." in rendered


def test_tracker_reuses_verified_session_context_without_exposing_it(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools

    f = fixture
    register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    source = f.registry.get_spec("sensor_read")
    source.input_schema = {"type": "object", "properties": {"session_id": {"type": "string"}}}
    probe = [{"tool_name": "sensor_read", "arguments": {}}]
    context = {
        "conversation_id": "web:alpha",
        "browser_session_scope": "setup-turn",
        "browser_session_owner": "conversation:web:alpha",
        "prepared_observations": {
            json.dumps(probe[0], sort_keys=True): {"reading": 23, "session_id": "physical-tab-a"}
        },
    }
    result = f.registry.invoke(ToolInvocation("create", "create_live_tracker", "web:alpha", {
        "name": "状態", "duration_seconds": 90, "probe": probe, "matcher": matcher("greater_than", 99),
        "probe_contexts": [{"browser_session_id": "untrusted-tab"}],
    }, capsule_id="setup-capsule", flow_context=context))
    assert result.status == "completed", result
    job = f.store.get(result.output["id"])
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    for _ in range(2):
        claimed = f.store.claim(1)
        if claimed is None:
            with f.store.connection() as db:
                db.execute("UPDATE live_trackers SET next_check_at='' WHERE id=?", (job["id"],))
            claimed = f.store.claim(1)
        engine.check_once(*claimed)
    assert len(f.readings) == 2
    assert all(i.flow_context["browser_session_id"] == "physical-tab-a" for i in f.readings)
    assert all(i.flow_context["browser_session_scope"] == "setup-turn" for i in f.readings)
    assert "physical-tab-a" not in json.dumps(public_tracker(job))


@pytest.mark.parametrize("option,seconds", [(1,3600),(2,7200),(3,14400),(4,0)])
def test_expiry_choices_are_bounded_atomic_and_repeatable(fixture, option, seconds):
    now = utcnow()
    a = create(fixture, now=now)
    end = now + timedelta(seconds=91)
    fixture.store.expire(end)
    expired = fixture.store.get(a["id"])
    offer = expired["extension_offer_id"]
    notice = fixture.store.claim_notice(end)
    assert all(f"{n}." in notice["text"] for n in (1,2,3,4))
    fixture.store.finish_notice(notice, True, end)
    job, changed = fixture.store.choose_extension(offer, option, "workspace_admin", "web:alpha", end)
    assert changed
    assert job["state"] == ("active" if seconds else "stopped")
    assert job["probe"] == a["probe"] and job["matcher"] == a["matcher"]
    assert job["interval_seconds"] == a["interval_seconds"]
    if seconds:
        assert datetime.fromisoformat(job["expires_at"]) == end + timedelta(seconds=seconds)
        assert fixture.store.claim(1, end) is not None, "SQL deadline must update with payload"
    again, changed = fixture.store.choose_extension(offer, option, "workspace_admin", "web:alpha", end + timedelta(seconds=1))
    assert not changed and again["expires_at"] == job["expires_at"]
    if seconds:
        fixture.store.expire(end + timedelta(seconds=seconds + 1))
        second = fixture.store.claim_notice(end + timedelta(seconds=seconds + 1))
        assert second and second["id"] != notice["id"] and second["kind"] == "expired"


def test_expiry_choices_enforce_chat_workspace_age_and_cancellation(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    end = now + timedelta(seconds=91)
    fixture.store.expire(end)
    offer = fixture.store.get(a["id"])["extension_offer_id"]
    for workspace, chat in [("workspace_other", "web:alpha"),("workspace_admin", "web:other")]:
        with pytest.raises(ValueError):
            fixture.store.choose_extension(offer, 1, workspace, chat, end)
    with pytest.raises(ValueError, match="expired"):
        fixture.store.choose_extension(offer, 1, "workspace_admin", "web:alpha", end + timedelta(hours=25))
    fixture.store.stop(a["id"], "workspace_admin")
    with pytest.raises(ValueError, match="no longer current"):
        fixture.store.choose_extension(offer, 1, "workspace_admin", "web:alpha", end)


def test_blocked_tracker_also_notifies_at_expiry(fixture):
    now = utcnow()
    a = create(fixture, now=now)
    for i in range(3):
        job, token = fixture.store.claim(1, now + timedelta(seconds=i*12))
        fixture.store.finish(a["id"], token, Observation("unknown"), now + timedelta(seconds=i*12))
    fixture.store.expire(now + timedelta(seconds=91))
    with fixture.store.connection() as db:
        notices = [dict(row) for row in db.execute("SELECT * FROM live_tracker_notifications WHERE kind='expired'")]
    assert len(notices) == 1 and "paused" in notices[0]["text"]


def test_extension_concurrent_replies_only_add_one_window(fixture):
    from concurrent.futures import ThreadPoolExecutor
    now=utcnow()
    job=create(fixture,now=now)
    end=now+timedelta(seconds=91)
    fixture.store.expire(end)
    ident=fixture.store.get(job["id"])["extension_offer_id"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _: fixture.store.choose_extension(ident,2,"workspace_admin","web:alpha",end), range(2)))
    assert sum(changed for _,changed in results)==1
    assert {j["expires_at"] for j,_ in results}=={(end+timedelta(hours=2)).isoformat()}


def test_setup_ack_is_durable_and_generation_bound(fixture):
    now=utcnow()
    job=create(fixture,now=now,require_start_receipt=True)
    assert fixture.store.confirm_start_receipt(job["id"],job["expires_at"])
    assert not LiveTrackerStore(fixture.store.path).get(job["id"])["require_start_receipt"]
    end=now+timedelta(seconds=91)
    fixture.store.expire(end)
    ident=fixture.store.get(job["id"])["extension_offer_id"]
    extended,_=fixture.store.choose_extension(ident,1,"workspace_admin","web:alpha",end)
    assert extended["require_start_receipt"]
    assert not fixture.store.confirm_start_receipt(job["id"],job["expires_at"])
    assert fixture.store.confirm_start_receipt(job["id"],extended["expires_at"])


def test_storage_busy_detects_sqlite_codes_not_error_prose(tmp_path):
    import sqlite3
    from nullion.live_tracking_choices import is_storage_busy
    path=tmp_path/"busy.db"
    first=sqlite3.connect(path)
    first.execute("CREATE TABLE value (n INTEGER)")
    first.execute("BEGIN IMMEDIATE")
    second=sqlite3.connect(path,timeout=0)
    try:
        with pytest.raises(sqlite3.OperationalError) as captured:
            second.execute("BEGIN IMMEDIATE")
        assert is_storage_busy(captured.value)
        assert not is_storage_busy(sqlite3.OperationalError("database is locked"))
        assert not is_storage_busy(ValueError("database is locked"))
    finally:
        first.rollback();first.close();second.close()


@pytest.mark.parametrize('condition', ['Watch the board', 'تابع اللوحة', 'ボードを確認', 'Vigila el tablero'])
def test_all_admitted_observations_fit_semantic_packet_without_truncation(condition):
    from nullion.live_tracking import MAX_PROBE_OUTPUT_CHARS, validate_prepared_observations
    outputs = [{'body': 'a' * (MAX_PROBE_OUTPUT_CHARS - 20)} for _ in range(3)]
    plan = [{'tool_name': f'read_{i}', 'arguments': {}} for i in range(3)]
    prepared = {json.dumps(step, ensure_ascii=False, sort_keys=True): out for step, out in zip(plan, outputs)}
    match = {'kind': 'semantic', 'instruction': condition}
    validate_prepared_observations(plan, match, prepared)
    calls = []
    def decide(**kw):
        packet = json.loads(kw['messages'][0]['content'][0]['text'])
        assert packet['observations'] == outputs
        calls.append(kw)
        return {'content': [{'type': 'text', 'text': '{"status":"no_match"}'}]}
    assert evaluate_semantic(match, outputs, SimpleNamespace(create=decide)).status == 'no_match'
    assert len(calls) == 1


def test_setup_rejects_over_budget_source_before_creating_a_tracker():
    from nullion.live_tracking import validate_prepared_observations
    plan = [{'tool_name': 'read', 'arguments': {}}]
    prepared = {json.dumps(plan[0], sort_keys=True): {'body': 'x' * 18000}}
    with pytest.raises(ValueError, match='too large'):
        validate_prepared_observations(plan, {'kind': 'semantic', 'instruction': '観測'}, prepared)


@pytest.mark.parametrize('payload,reason', [
    ({'status':'unknown','reason':'missing_data'}, 'missing_data'),
    ({'status':'unknown','reason':'stale_data'}, 'stale_data'),
    ({'status':'unknown','reason':'ambiguous_data'}, 'ambiguous_data'),
    ({'status':'unknown','reason':'UNTRUSTED SECRET'}, 'condition_unknown'),
    ({'status':'wrong'}, 'evaluation_invalid'),
    ({'status':'matched', 'evidence':[]}, 'evidence_invalid'),
])
def test_evaluator_preserves_typed_cause_without_echoing_provider_prose(payload, reason):
    out = evaluate_semantic({'kind':'semantic','instruction':'観測'}, [{'value':1}],
        SimpleNamespace(create=lambda **kw: {'content':[{'type':'text','text':json.dumps(payload)}]}))
    assert out.status == 'unknown' and out.reason == reason
    from nullion.live_tracking import observation_problem
    problem = observation_problem(out)
    assert problem['message'] and problem['remedy']
    assert 'UNTRUSTED' not in json.dumps(problem)


def test_evaluator_timeout_has_a_specific_repair_message():
    from nullion.model_clients import ModelClientTimeoutError
    def timeout(**kw):
        raise ModelClientTimeoutError('not public')
    out = evaluate_semantic({'kind':'semantic','instruction':'観測'}, [{}], SimpleNamespace(create=timeout))
    assert out.reason == 'evaluation_timeout'


def test_pause_notice_explains_size_limit_and_survives_delivery_delay(fixture):
    now = utcnow()
    a = create(fixture, now=now, expires_at=now+timedelta(minutes=30))
    for _ in range(3):
        job, token = fixture.store.claim(1, now)
        fixture.store.finish(a['id'], token, Observation('unknown', reason='observation_too_large',
            diagnostics={'observed_chars':21000,'limit_chars':18000,'tool_name':'sensor_read','step':0}), now)
        now = datetime.fromisoformat(fixture.store.get(a['id'])['next_check_at'])
    job = fixture.store.get(a['id'])
    assert job['state'] == 'blocked' and job['checks'] == 3
    assert public_tracker(job)['last_problem']['code'] == 'observation_too_large'
    notice = fixture.store.claim_notice(now+timedelta(minutes=6))
    assert notice['kind'] == 'blocked'
    assert '21,000' in notice['text'] and '18,000' in notice['text']
    assert 'How to fix it**' in notice['text'] and 'narrow the saved source' in notice['text']
    assert fixture.store.claim(1, now+timedelta(minutes=6)) is None
    fixture.store.resume(a['id'], 'workspace_admin', now+timedelta(minutes=6))
    assert fixture.store.get(a['id'])['last_problem'] is None
    assert not fixture.store.notice_is_current(notice, now+timedelta(minutes=6))


def test_retry_backoff_does_not_poll_faster_than_requested(fixture):
    now=utcnow()
    a=create(fixture,now=now,interval_seconds=300,expires_at=now+timedelta(hours=1))
    for _ in range(3):
        job,token=fixture.store.claim(1,now)
        fixture.store.finish(a['id'],token,Observation('unknown'),now)
        due=datetime.fromisoformat(fixture.store.get(a['id'])['next_check_at'])
        assert (due-now).total_seconds() >= 300
        now=due


@pytest.mark.parametrize('kind,pointer,value,status',[
    ('quote','/content','"slot": "B7"','matched'),
    ('quote','/content','"slot": "C9"','unknown'),
    ('quote','/content','   ','unknown'),
    ('quote','/content/slot','B7','unknown'),
    ('value','/content','B7','unknown'),
    ('unsupported','/content','B7','unknown'),
])
def test_semantic_quotes_are_verified_against_the_original_string(kind,pointer,value,status):
    payload={'status':'matched','summary':'Platform B7 is assigned.',
             'evidence':[{'step':0,'pointer':pointer,'kind':kind,'value':value}]}
    out=evaluate_semantic({'kind':'semantic','instruction':'確認'},
        [{'content':json.dumps({'slot':'B7','details':'x'*14000})}],
        SimpleNamespace(create=lambda **kw:{'content':[{'type':'text','text':json.dumps(payload)}]}))
    assert out.status==status


def test_browser_transport_failure_metadata_survives_through_tracker_pause(fixture):
    from urllib.error import URLError
    from nullion.plugins.browser_plugin.browser_tools import _browser_failure_output
    failure=_browser_failure_output(URLError(ConnectionRefusedError(61,'not public')))
    assert failure=={'failure_reason':'browser_connection_unavailable'}
    assert _browser_failure_output(TimeoutError())=={'failure_reason':'source_timeout','failure_scope':'invocation'}
    assert _browser_failure_output(RuntimeError('Connection refused'))=={'failure_reason':'source_error'}
    f=fixture
    f.registry._handlers['sensor_read']=lambda inv: ToolResult(inv.invocation_id,inv.tool_name,'failed',failure,'private raw endpoint')
    a=create(f)
    engine=LiveTrackerEngine(f.store,registry=f.registry,runtime_store=f.runtime.store,deliver=lambda *args:True)
    now=utcnow()
    for _ in range(3):
        job,token=f.store.claim(1,now)
        outcome=engine._observe({'job':job})['outcome']
        f.store.finish(a['id'],token,outcome,now)
        now=datetime.fromisoformat(f.store.get(a['id'])['next_check_at'])
    notice=f.store.claim_notice(now)
    assert 'browser connection' in notice['text'] and 'reconnect the browser' in notice['text']
    assert 'private raw endpoint' not in notice['text'] and 'not public' not in notice['text']


def test_identical_live_notice_copy_keeps_distinct_durable_delivery_ids(tmp_path):
    from nullion.chat_store import ChatStore
    store=ChatStore(tmp_path/'chat.db',key_path=tmp_path/'chat.key')
    for ident in ['one:expired','two:expired','two:expired']:
        store.save_message('web:notice-test','bot','観測期間が終了しました',metadata={
            'source':'live_tracker','live_notice_id':ident,'live_tracker_extension_choice':ident+':extension'})
    rows=store.load_messages('web:notice-test')
    assert [r['metadata']['live_notice_id'] for r in rows]==['one:expired','two:expired']


def test_semantic_evaluator_profiles_without_mutating_chat_client():
    from dataclasses import dataclass
    from nullion.live_tracking import EVALUATION_TIMEOUT_SECONDS, EVALUATION_MAX_TOKENS
    calls = []
    @dataclass
    class Model:
        reasoning_effort: str = 'high'
        model: str = 'only-configured-model'
        def create(self, **kw):
            calls.append((self.reasoning_effort, self.model, kw))
            if self.reasoning_effort == 'high' or kw['max_tokens'] < 1024:
                return {'completion': {'status':'incomplete','reason':'max_output_tokens',
                                      'usage':{'output_tokens':450,'reasoning_tokens':450}}, 'content':[]}
            return {'content':[{'type':'text','text':'{"status":"no_match"}'}]}
    client=Model()
    for _ in range(4):
        out=evaluate_semantic({'kind':'semantic','instruction':'観測'}, [{'slots':[]}],client)
        assert out.status=='no_match'
        assert out.diagnostics['evaluation_elapsed_ms']>=0
    assert client.reasoning_effort=='high'
    assert len(calls)==4
    assert all(effort=='low' and name==client.model and kw['timeout']==EVALUATION_TIMEOUT_SECONDS
               and kw['max_tokens']==EVALUATION_MAX_TOKENS for effort,name,kw in calls)


@pytest.mark.parametrize('status', ['no_match','matched'])
def test_truncated_evaluator_never_accepts_even_valid_partial_json(status):
    response={'content':[{'type':'text','text':json.dumps({'status':status,
              'summary':'Ready', 'evidence':[{'step':0,'pointer':'/slot','value':'B7'}]})}],
              'completion':{'status':'incomplete','reason':'max_output_tokens',
                            'usage':{'output_tokens':2048,'reasoning_tokens':2030}}}
    out=evaluate_semantic({'kind':'semantic','instruction':'確認'}, [{'slot':'B7'}],
                          SimpleNamespace(create=lambda **kw: response))
    assert out.status=='unknown' and out.reason=='evaluation_truncated'
    assert out.diagnostics['reasoning_tokens']==2030
    from nullion.live_tracking import observation_problem
    assert 'response budget' in observation_problem(out)['message']


def test_evaluator_timings_persist_for_success_and_failure(fixture):
    a=create(fixture)
    job,token=fixture.store.claim(1)
    outcome=Observation('no_match',diagnostics={'evaluation_elapsed_ms':11001,'timeout_seconds':30,'max_tokens':2048})
    fixture.store.finish(a['id'],token,outcome)
    saved=fixture.store.get(a['id'])
    assert saved['last_evaluation']['evaluation_elapsed_ms']==11001
    assert saved['last_evaluation']['status']=='no_match' and saved['last_problem'] is None


def test_engine_reuses_evaluator_authentication_state(fixture):
    from dataclasses import dataclass
    @dataclass
    class Provider:
        reasoning_effort:str='high'
        refresh_generation:int=0
        def create(self, **kw):
            self.refresh_generation+=1
            return {'content':[{'type':'text','text':'{"status":"no_match"}'}]}
    parent=Provider()
    engine=LiveTrackerEngine(fixture.store,registry=fixture.registry,runtime_store=fixture.runtime.store,
                             deliver=lambda *args: True,model_client=parent)
    for _ in range(3):
        assert evaluate_semantic({'kind':'semantic','instruction':'監視'},[{}],engine.model_client).status=='no_match'
    assert engine.model_client.refresh_generation==3
    assert engine.model_client.reasoning_effort=='low'
    assert parent.refresh_generation==0 and parent.reasoning_effort=='high'


@pytest.mark.parametrize('adapter', ['chat','messages'])
def test_other_adapters_preserve_truncation_metadata(adapter):
    from nullion.model_clients import OpenAIChatCompletionsModelClient, AnthropicMessagesModelClient
    if adapter=='messages':
        response=SimpleNamespace(stop_reason='max_tokens',content=[SimpleNamespace(type='text',text='{"status":"no_match"}')])
        output=AnthropicMessagesModelClient._parse_response(response)
    else:
        choice=SimpleNamespace(finish_reason='length',message=SimpleNamespace(content='{"status":"no_match"}',tool_calls=[]))
        transport=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=lambda **kw: SimpleNamespace(choices=[choice]))))
        client=OpenAIChatCompletionsModelClient(client=transport,model='example')
        output=client.create(messages=[],tools=[])
    assert output['completion']=={'status':'incomplete','reason':'max_output_tokens'}
    out=evaluate_semantic({'kind':'semantic','instruction':'監視'},[{}],SimpleNamespace(create=lambda **kw: output))
    assert out.status=='unknown' and out.reason=='evaluation_truncated'


@pytest.mark.parametrize('kind',['chat','messages'])
def test_evaluator_preserves_clients_without_explicit_reasoning(kind):
    from nullion.model_clients import OpenAIChatCompletionsModelClient, AnthropicMessagesModelClient
    calls=[]
    def create(**kw):
        calls.append(kw)
        assert 'reasoning_effort' not in kw and 'thinking' not in kw
        if kind=='chat':
            return SimpleNamespace(choices=[SimpleNamespace(finish_reason='stop',message=SimpleNamespace(content='{"status":"no_match"}',tool_calls=[]))])
        return SimpleNamespace(stop_reason='end_turn',content=[SimpleNamespace(type='text',text='{"status":"no_match"}')])
    transport=SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)),
                              messages=SimpleNamespace(create=create))
    cls=OpenAIChatCompletionsModelClient if kind=='chat' else AnthropicMessagesModelClient
    client=cls(client=transport,model='legacy-model')
    assert evaluate_semantic({'kind':'semantic','instruction':'確認'},[{}],client).status=='no_match'
    assert len(calls)==1 and client.reasoning_effort is None


@pytest.mark.parametrize('scope,complete,truncated,expected', [
    ('section-a', True, True, 'no_match'),
    ('section-a', False, True, 'unknown'),
    ('section-missing', True, False, 'unknown'),
    (None, True, False, 'no_match'),
    (None, True, True, 'unknown'),
    ('omitted', True, False, 'unknown'),
])
def test_collection_negative_requires_verified_complete_scope(scope, complete, truncated, expected):
    output = {'result_kind': 'browser_item_collection', 'coverage': {
        'scope_found': True, 'truncated': truncated,
        'sections': [{'id': 'section-a', 'heading': '進展', 'complete': complete}]},
        'items': [{'title': '既知の記事', 'section_id': 'section-a'}]}
    decision = {'status': 'no_match'}
    if scope != 'omitted': decision['checked_scopes'] = [{'step': 0, 'section_id': scope}]
    class Model:
        def create(self, **kwargs):
            assert 'Section headings apply' in kwargs['system']
            return {'content': [{'type': 'text', 'text': json.dumps(decision)}]}
    result = evaluate_semantic({'instruction': '新しい記事'}, [output], Model())
    assert result.status == expected
    if expected == 'unknown': assert result.reason == 'coverage_incomplete'
    assert result.diagnostics['source_coverage'][0]['coverage']['sections'][0]['heading'] == '進展'


def test_collection_scope_cannot_be_forged_or_borrowed_from_another_step():
    output = {'result_kind': 'browser_item_collection', 'coverage': {
        'scope_found': True, 'truncated': True, 'sections': [
            {'id': 'section-a', 'heading': '進展', 'complete': True}]}}
    from nullion.live_tracking import _negative_scope_is_complete
    for scopes in [None, [], [{'step': True, 'section_id': 'section-a'}],
                   [{'step': 1, 'section_id': 'section-a'}], [{'step': 0}],
                   [{'step': 0, 'section_id': 'invented'}]]:
        assert not _negative_scope_is_complete([output], {'checked_scopes': scopes})


@pytest.mark.parametrize('status,expected', [('unknown','failed'),('no_match','completed')])
def test_semantic_setup_rejects_unusable_prepared_source(fixture,status,expected):
    from nullion.live_tracking_tools import register_live_tracker_tools
    f=fixture
    class Model:
        def create(self, **kwargs):
            return {'content':[{'type':'text','text':json.dumps({'status':status,'reason':'missing_data'})}]}
    register_live_tracker_tools(f.registry)
    register_live_tracker_tools(f.registry,model_client_getter=lambda:Model())
    assert f.registry.get_spec('create_live_tracker').timeout_seconds > 30
    f.registry.live_tracker_store_path=f.store.path
    probe=[{'tool_name':'sensor_read','arguments':{}}]
    context={'conversation_id':'web:alpha','prepared_observations':{
        json.dumps(probe[0],sort_keys=True):{'reading':23}}}
    result=f.registry.invoke(ToolInvocation('prepare','create_live_tracker','web:alpha',{
        'name':'観測','duration_seconds':90,'probe':probe,
        'matcher':{'kind':'semantic','instruction':'必要な値を通知'}},flow_context=context))
    assert result.status==expected
    assert len(f.store.list('workspace_admin'))==(1 if status=='no_match' else 0)
    if status=='unknown':
        assert result.output['validation']['code']=='semantic_condition_unresolved'
        assert result.output['validation']['problem']['code']=='missing_data'


def test_restart_saved_setup_is_bounded_scoped_atomic_and_idempotent(fixture):
    f = fixture
    now = utcnow()
    job = create(f, now=now)
    end = datetime.fromisoformat(job['expires_at'])
    f.store.expire(end)
    before = f.store.get(job['id'])
    with pytest.raises(ValueError, match='original chat'):
        f.store.restart(job['id'], 'workspace_admin', 'web:other', 60, 'restart-a', now=end)
    for invalid in [None, True, 0, 10**100]:
        with pytest.raises(ValueError):
            f.store.restart(job['id'], 'workspace_admin', 'web:alpha', invalid, 'restart-a', now=end)
    restarted = f.store.restart(job['id'], 'workspace_admin', 'web:alpha', 60, 'restart-a', now=end)
    assert restarted['id'] == before['id'] and restarted['state'] == 'active'
    assert restarted['probe'] == before['probe'] and restarted['matcher'] == before['matcher']
    assert restarted['checks'] == before['checks'] and restarted['extension_count'] == 1
    assert f.store.extension_choice_committed(restarted)
    again = f.store.restart(job['id'], 'workspace_admin', 'web:alpha', 60, 'restart-a', now=end+timedelta(seconds=10))
    assert again['expires_at'] == restarted['expires_at'] and again['extension_count'] == 1
    # A later numeric continuation must not be checked against the earlier restart.
    f.store.expire(end+timedelta(seconds=61))
    expired = f.store.get(job['id'])
    extended, changed = f.store.choose_extension(expired['extension_offer_id'], 1, 'workspace_admin', 'web:alpha', now=end+timedelta(seconds=62))
    assert changed and f.store.extension_choice_committed(extended)
    assert 'window_restart_key' not in extended


def test_expiry_delivery_survives_an_offline_service_past_five_minutes(fixture):
    job = create(fixture)
    end = datetime.fromisoformat(job['expires_at'])
    fixture.store.expire(end)
    notice = fixture.store.claim_notice(end+timedelta(minutes=20))
    assert notice and notice['kind'] == 'expired'
    assert '1. Continue' in notice['text']
    fixture.store.finish_notice(notice, False, end+timedelta(minutes=20))
    retry = fixture.store.claim_notice(end+timedelta(minutes=21))
    assert retry['id'] == notice['id']
    fixture.store.finish_notice(retry, True, end+timedelta(minutes=21))
    assert fixture.store.claim_notice(end+timedelta(minutes=22)) is None
    assert fixture.store.get(job['id'])['notification_status'] == 'sent'


def test_expired_resume_returns_actionable_menu_instead_of_invalid_plan(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools, clarification_reply
    f = fixture
    register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    past = utcnow()-timedelta(minutes=3)
    job = create(f, now=past)
    f.store.expire()
    result = f.registry.invoke(ToolInvocation('renew','resume_live_tracker','web:alpha',{'id':job['id']}))
    assert result.status == 'failed'
    assert result.output['reason'] == 'live_tracking_needs_details'
    assert result.output['extension_choice_id']
    assert '1. Continue' in clarification_reply([result])
    success = f.registry.invoke(ToolInvocation('renew2','resume_live_tracker','web:alpha',{'id':job['id'],'duration_seconds':60}))
    assert success.status == 'completed', success
    assert success.output['action_receipt']['action'] == 'extended'
    assert clarification_reply([result,success]) is None


def test_expired_tracker_without_expiry_alert_can_offer_numbered_renewal(fixture):
    past=utcnow()-timedelta(minutes=3)
    job=create(fixture,now=past,notify_on_expiry=False)
    fixture.store.expire()
    assert fixture.store.claim_notice() is None
    offer=fixture.store.renewal_offer(job['id'],'workspace_admin','web:alpha')
    assert fixture.store.renewal_offer(job['id'],'workspace_admin','web:alpha')['id']==offer['id']
    renewed,changed=fixture.store.choose_extension(offer['id'],2,'workspace_admin','web:alpha')
    assert changed and renewed['state']=='active' and renewed['notify_on_expiry'] is False


@pytest.mark.parametrize('field,value,code',[
    ('step',1,'invalid_observation_step'),('step',True,'invalid_observation_step'),
    ('pointer','text','invalid_observation_pointer'),('op','contains','invalid_comparison_operator'),
])
def test_condition_validation_identifies_the_exact_contract_field(fixture,field,value,code):
    from nullion.live_tracking import LiveTrackerPlanError
    condition=matcher();condition['conditions'][0][field]=value
    with pytest.raises(LiveTrackerPlanError) as caught:
        validate_plan([{'tool_name':'sensor_read','arguments':{}}],condition,fixture.registry)
    assert caught.value.validation['code']==code
    assert caught.value.validation['path']==f'/matcher/conditions/0/{field}'


def test_preparation_admission_reports_oversized_and_uses_unicode_polling_budget():
    from nullion.live_tracking import observation_preparation,MAX_PROBE_OUTPUT_CHARS,validate_prepared_observations,LiveTrackerPlanError
    step={'tool_name':'観測','arguments':{'region':'記事'}}
    key=json.dumps(step,sort_keys=True,ensure_ascii=False)
    output={'text':'観測'*4000}
    assert len(json.dumps(output))>MAX_PROBE_OUTPUT_CHARS
    assert observation_preparation(step,output)['status']=='ready'
    large={'text':'a'*10000,'record':{'text':'a'*10000}}
    receipt=observation_preparation(step,large)
    assert receipt['status']=='needs_narrower_source' and receipt['observed_chars']>receipt['limit_chars']
    with pytest.raises(LiveTrackerPlanError) as caught:
        validate_prepared_observations([step],{'kind':'semantic','instruction':'観測'}, {}, {key:receipt})
    assert caught.value.validation['code']=='observation_too_large'
    assert observation_preparation(step,{'value':float('nan')})['status']=='unsupported_source_output'


def test_success_resets_only_matching_browser_precondition_failure_streak():
    from nullion.agent_orchestrator import _reset_repaired_precondition_streaks
    def key(tool,session,**other):return json.dumps({'tool_name':tool,'session_id':session,**other})
    repaired=key('browser_extract_items','one',repair_kind='repair_precondition')
    other=key('browser_extract_items','two',repair_kind='repair_precondition')
    transport=key('browser_extract_items','one',failure_scope='source')
    different=key('browser_extract_text','one',repair_kind='repair_precondition')
    state={repaired:1,other:1,transport:2,different:1,'[]':1}
    result=ToolResult('read','browser_extract_items','completed',{'session_id':'one'})
    assert _reset_repaired_precondition_streaks(state,result)=={k:v for k,v in state.items() if k!=repaired}
    for status,session in [('failed','one'),('completed','three'),('completed',None)]:
        assert _reset_repaired_precondition_streaks(state,ToolResult('read','browser_extract_items',status,{'session_id':session}))==state


def test_semantic_history_uses_declared_identity_and_survives_reordering_edits_and_empty_baseline():
    from nullion.live_tracking import observation_identity_history
    def output(rows):return {'records':rows,'source_record_collections':[{'path':['records'],'change_identity_fields':['識別子']}]}
    first,keys=observation_identity_history([output([{'識別子':'one','headline':'First'}])])
    assert first['available'] and first['records'][0]['status']=='baseline'
    second,more=observation_identity_history([output([{'識別子':'two'},{'識別子':'one','headline':'Edited'}])],keys,True)
    assert [r['status'] for r in second['records']]==['new','seen_before'] and keys[0] in more
    empty,keys=observation_identity_history([output([])])
    assert empty['available'] and empty['records']==[] and keys==()
    after,_=observation_identity_history([output([{'識別子':'first-after-empty'}])],keys,True)
    assert after['records'][0]['status']=='new'
    unavailable,_=observation_identity_history([{'source_record_collections':None}])
    assert not unavailable['available']


def test_projected_canonical_url_match_identity_does_not_change_with_record_order():
    from nullion.live_tracking import _semantic_match_keys
    url='https://example.test/news/article'
    first=_semantic_match_keys([{'step':0,'pointer':'/items/0/title','value':'Original'}],
        [{'result_kind':'browser_item_collection','items':[{'canonical_url':url,'title':'Original'}]}])
    reordered=_semantic_match_keys([{'step':0,'pointer':'/items/1/title','value':'Edited'}],
        [{'result_kind':'browser_item_collection','items':[{'canonical_url':'https://example.test/news/other'},{'canonical_url':url,'title':'Edited'}]}])
    assert first==reordered


@pytest.mark.parametrize('refresh,restart,lose_page,expected', [
    (None, False, False, ['navigate', 'read', 'navigate', 'read']),
    ('missing_saved_policy', False, False, ['navigate', 'read', 'navigate', 'read']),
    ('reuse', False, False, ['navigate', 'read', 'read']),
    ('navigate', False, False, ['navigate', 'read', 'navigate', 'read']),
    ('reuse', True, False, ['navigate', 'read', 'navigate', 'read']),
    ('reuse', False, True, ['navigate', 'read', 'navigate', 'read']),
])
def test_worker_bootstrap_refresh_restart_and_lost_page(fixture, refresh, restart, lose_page, expected):
    """Creator handles are process-local; a new worker has no creator page."""
    from nullion.live_tracking_tools import register_live_tracker_tools
    f = fixture
    calls = []
    worker_pages = {}
    def navigate(inv):
        session = inv.flow_context['browser_session_id']
        calls.append(('navigate', session, inv.arguments['url']))
        worker_pages[session] = len(calls)
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'page': inv.arguments['url']})
    def read(inv):
        session = inv.flow_context['browser_session_id']
        if session not in worker_pages:
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {'observation_setup_required': True})
        assert session in worker_pages and session != 'creator-tab'
        calls.append(('read', session, worker_pages[session]))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'reading': 23})
    f.registry.register(ToolSpec('source_open', 'Open source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), navigate)
    f.registry.register(ToolSpec('portable_read', 'Read source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
        observation_setup_tools=('source_open',)), read)
    register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    step = {'tool_name': 'portable_read', 'arguments': {}}
    prepared = {'reading': 23, 'session_id': 'creator-tab',
        'observation_setup': [{'tool_name': 'source_open', 'arguments': {'url': 'https://example.org/news'}}]}
    result = f.registry.invoke(ToolInvocation('create-portable', 'create_live_tracker', 'web:alpha', {
        'name': '状態', 'duration_seconds': 90, 'interval_seconds': 5, 'probe': [step],
        **({'source_refresh': refresh} if refresh not in {None, 'missing_saved_policy'} else {}),
        'matcher': matcher('greater_than', 99)}, capsule_id='portable', flow_context={
            'conversation_id': 'web:alpha', 'browser_session_id': 'creator-tab',
            'prepared_observations': {json.dumps(step, ensure_ascii=False, sort_keys=True): prepared}}))
    assert result.status == 'completed', result
    job = f.store.get(result.output['id'])
    expected_refresh = refresh if refresh not in {None, 'missing_saved_policy'} else 'navigate'
    assert job['probe_contexts'] == [{'observation_setup': prepared['observation_setup'], 'observation_refresh': expected_refresh}]
    if refresh == 'missing_saved_policy':
        # Older portable recipes may not have stored a refresh policy. A
        # missing value must not silently turn a static source into a live DOM.
        job['probe_contexts'][0].pop('observation_refresh')
        with f.store.connection() as db:
            db.execute('UPDATE live_trackers SET payload=? WHERE id=?', (json.dumps(job), job['id']))
    f.store.confirm_start_receipt(job['id'], job['expires_at'])
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    for index in range(2):
        if index and (restart or lose_page):
            worker_pages.clear()
        if index and restart:
            engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
        with f.store.connection() as db:
            db.execute("UPDATE live_trackers SET next_check_at='' WHERE id=?", (job['id'],))
        engine.check_once(*f.store.claim(1))
    assert [c[0] for c in calls] == expected
    assert len({c[1] for c in calls}) == 1
    assert f.store.get(job['id'])['checks'] == 2


def test_terminal_tracker_releases_only_its_own_source_resources(fixture):
    f = fixture
    released = []
    f.registry.register_cleanup_hook(lambda scope: released.append(scope))
    job = create(f)
    other = create(f, request_key='unrelated', name='別の観測')
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    engine._owned_sources.update((job['id'], other['id']))
    f.store.stop(job['id'], 'workspace_admin')
    engine._release_inactive_sources()
    assert released == ['live-tracker:' + job['id']]
    assert engine._owned_sources == {other['id']}
    engine.stop()
    assert released[-1] == 'live-tracker:' + other['id'] and not engine._owned_sources


def test_portable_reuse_keeps_two_sources_in_distinct_stable_sessions(fixture):
    from nullion.live_tracking_tools import register_live_tracker_tools
    f = fixture
    pages = {'unrelated': 'do-not-touch'}
    opened, read = [], []
    values = {'https://example.org/a': 23, 'https://example.org/b': 37}
    def open_source(inv):
        session = inv.flow_context['browser_session_id']
        pages[session] = inv.arguments['url']
        opened.append((session, inv.arguments['url']))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {})
    def read_source(inv):
        session = inv.flow_context['browser_session_id']
        url = pages[session]
        read.append((session, url))
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'reading': values[url]})
    f.registry.register(ToolSpec('open_source', 'Open source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2), open_source)
    f.registry.register(ToolSpec('pair_read', 'Read source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
                                observation_setup_tools=('open_source',)), read_source)
    register_live_tracker_tools(f.registry)
    f.registry.live_tracker_store_path = f.store.path
    probe = [{'tool_name': 'pair_read', 'arguments': {'source': key}} for key in ('a', 'b')]
    prepared = {json.dumps(step, sort_keys=True): {'reading': values['https://example.org/' + key],
                'observation_setup': [{'tool_name': 'open_source', 'arguments': {'url': 'https://example.org/' + key}}]}
                for step, key in zip(probe, ('a', 'b'))}
    result = f.registry.invoke(ToolInvocation('create-pair', 'create_live_tracker', 'web:alpha', {
        'name': '比較', 'duration_seconds': 90, 'interval_seconds': 5, 'source_refresh': 'reuse',
        'probe': probe, 'matcher': matcher('greater_than', 99)}, flow_context={
            'conversation_id': 'web:alpha', 'prepared_observations': prepared}))
    assert result.status == 'completed', result
    job = f.store.get(result.output['id'])
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    for _ in range(2):
        observed = engine._observe({'job': job})
        assert [row['reading'] for row in observed['outputs']] == [23, 37], observed
    assert len(opened) == 2 and len({session for session, _ in read}) == 2
    assert [url for _, url in read] == list(values) * 2
    assert pages['unrelated'] == 'do-not-touch'


def test_source_cleanup_db_contention_is_retried_without_aborting(fixture, monkeypatch):
    import sqlite3
    f = fixture
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, deliver=lambda *a: True)
    engine._owned_sources.add('owned-job')
    monkeypatch.setattr(f.store, 'get', lambda ident: (_ for _ in ()).throw(sqlite3.OperationalError('database is locked')))
    engine._release_inactive_sources()
    assert engine._owned_sources == {'owned-job'}


@pytest.mark.parametrize('proposal,usable,repair_saved', [
    ({'scope_preserved': True, 'updates': {'selector': '#new'}}, True, True),
    ({'scope_preserved': True, 'updates': {'selector': '#new'}}, False, False),
    ({'scope_preserved': True, 'updates': {'selector': '#new', 'account': 'another'}}, True, False),
    ({'scope_preserved': True, 'updates': {'url': 'https://another.example'}}, True, False),
    ({'scope_preserved': False, 'updates': {'selector': '#new'}}, True, False),
])
def test_source_repair_is_read_only_and_saved_only_after_condition_verification(fixture, proposal, usable, repair_saved):
    f = fixture
    calls = []
    def read(inv):
        calls.append(inv)
        if inv.arguments['selector'] == '#old':
            return ToolResult(inv.invocation_id, inv.tool_name, 'failed', {
                'reason': 'tool_precondition_unmet', 'failure_reason': 'source_scope_missing',
                'recovery': {'kind': 'repair_precondition', 'retry_tool_name': inv.tool_name,
                    'required_tool_names': ['inspect_source']}}, 'The saved region is unavailable')
        return ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'reading': 23} if usable else {'unusable': True})
    f.registry.register(ToolSpec('scoped_read', 'Read verified region', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2,
        input_schema={'type': 'object', 'properties': {'selector': {'type': 'string'}, 'account': {'type': 'string'}}, 'required': ['selector']},
        observation_repair_parameters=('selector',)), read)
    f.registry.register(ToolSpec('inspect_source', 'Inspect source', ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 2),
        lambda inv: ToolResult(inv.invocation_id, inv.tool_name, 'completed', {'regions': ['#new']}))
    class Model:
        calls = 0
        def create(self, **kw):
            self.calls += 1
            assert kw['tools'] == [] and kw['timeout'] == 20
            packet = json.loads(kw['messages'][0]['content'][0]['text'])
            assert packet['condition'] == matcher('greater_than', 99)
            assert packet['inspections'][0]['evidence'] == {'regions': ['#new']}
            return {'content': [{'type': 'text', 'text': json.dumps(proposal)}], 'stop_reason': 'end_turn'}
    model = Model()
    job = create(f, probe=[{'tool_name': 'scoped_read', 'arguments': {'selector': '#old', 'account': 'original'}}], matcher=matcher('greater_than', 99))
    engine = LiveTrackerEngine(f.store, registry=f.registry, runtime_store=f.runtime.store, model_client=model, deliver=lambda *a: True)
    engine.check_once(*f.store.claim(1))
    saved = f.store.get(job['id'])
    assert model.calls == 1
    assert saved['probe'][0]['arguments'] == {'selector': '#new' if repair_saved else '#old', 'account': 'original'}
    assert saved['matcher'] == job['matcher'] and saved['probe_contexts'] == job['probe_contexts']
    assert all(i.principal_id == job['principal_id'] and i.flow_context['request_id'] == 'live-tracker:' + job['id'] for i in calls)
    if repair_saved:
        with f.store.connection() as db: db.execute("UPDATE live_trackers SET next_check_at='' WHERE id=?", (job['id'],))
        engine.check_once(*f.store.claim(1))
        assert model.calls == 1 and f.store.get(job['id'])['checks'] == 2
    else:
        assert saved['consecutive_errors'] == 1 and saved['last_problem']


def test_pause_diagnostics_keep_typed_repair_cause_without_provider_payload():
    from nullion.live_tracking import observation_problem
    problem = observation_problem(Observation('source_error', reason='source_scope_missing', diagnostics={
        'source_recovery': {'status': 'model_unavailable', 'raw_response': 'private account payload'}}))
    assert problem['source_recovery'] == {'status': 'model_unavailable'}
    assert 'model connection' in problem['message'] and 'Settings' in problem['remedy']
    assert 'private' not in json.dumps(problem)
