from __future__ import annotations

import inspect
from pathlib import Path
import time
from types import SimpleNamespace

import nullion.web_app as web_app
from nullion.web_app import (
    _HTML,
    _version_tag,
    _approval_display_from_turn_result,
    create_app,
    _filter_supported_media_models,
    _invalid_media_model_capabilities,
    _media_model_options,
    _media_model_supports,
    _media_selection_supported,
    _normalize_media_models,
)


def test_media_provider_toggle_defaults_off_until_explicitly_enabled() -> None:
    options = _media_model_options(
        "image_ocr",
        provider_models={},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={},
        providers_configured={"openai": True},
    )

    assert options == []


def test_media_provider_toggle_exposes_models_when_explicitly_enabled() -> None:
    options = _media_model_options(
        "image_ocr",
        provider_models={},
        media_models={"openai": [{"model": "gpt-4o", "capabilities": ["image_input"]}]},
        providers_enabled={"openai": True},
        media_providers_enabled={"openai": True},
        providers_configured={"openai": True},
    )

    assert options == [{"provider": "openai", "model": "gpt-4o", "label": "openai · gpt-4o"}]


def test_chat_provider_model_fallback_also_requires_media_provider_enabled() -> None:
    disabled_options = _media_model_options(
        "image_ocr",
        provider_models={"anthropic": "claude-sonnet-4-6"},
        media_models={},
        providers_enabled={"anthropic": True},
        media_providers_enabled={},
        providers_configured={"anthropic": True},
    )
    enabled_options = _media_model_options(
        "image_ocr",
        provider_models={"anthropic": "claude-sonnet-4-6"},
        media_models={},
        providers_enabled={"anthropic": True},
        media_providers_enabled={"anthropic": True},
        providers_configured={"anthropic": True},
    )

    assert disabled_options == []
    assert enabled_options == [
        {"provider": "anthropic", "model": "claude-sonnet-4-6", "label": "anthropic · claude-sonnet-4-6", "connected": "false"}
    ]


def test_media_provider_enabled_javascript_is_not_default_true() -> None:
    assert "function mediaProviderEnabled(provider)" in _HTML
    assert "return _mediaProvidersEnabled[provider] === true;" in _HTML
    assert "return _mediaProvidersEnabled[provider] !== false;" not in _HTML


def test_header_shows_current_version_tag() -> None:
    assert '<span class="version-tag" title="Current Nullion version">' in _HTML
    assert _version_tag() in _HTML


def test_task_status_card_ignores_progress_notes() -> None:
    assert "const statusKind = String(data.status_kind || 'task_summary');" in _HTML
    assert "if (statusKind !== 'task_summary') return;" in _HTML
    assert 'if planner_feed_mode == "task" and status_kind != "task_summary":' in inspect.getsource(create_app)


def test_task_status_card_marks_terminal_list_as_finalizing() -> None:
    assert "const rowStates = [];" in _HTML
    assert "const allTerminal = rowStates.length > 0 && rowStates.every(state => ['complete', 'failed', 'cancelled'].includes(state));" in _HTML
    assert "? 'Finalizing results' : titles[0];" in _HTML


def test_resolved_approval_failure_state_wraps_without_squashing_title() -> None:
    assert ".approval-bubble.approval-resolved .approval-title { font-size: 12px; white-space: nowrap; }" in _HTML
    assert ".approval-bubble.approval-resolved .approval-state.visible" in _HTML
    assert "white-space: normal; overflow-wrap: anywhere;" in _HTML
    assert ".approval-bubble.approval-resolved .approval-state.visible { font-size: 12px; white-space: nowrap; }" not in _HTML


def test_decision_history_header_keeps_status_badge_on_one_line() -> None:
    assert "grid-template-columns: 24px minmax(0, 1fr) max-content" in _HTML
    assert ".decision-title {\n    min-width: 0;" in _HTML
    assert "letter-spacing: 0.06em; white-space: nowrap; border-radius: 999px;" in _HTML


def test_delivery_receipts_panel_and_api(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import web_app
    from nullion.tools import ToolRegistry

    receipts = [
        {
            "channel": "telegram",
            "target_id": "123",
            "status": "failed",
            "attachment_count": 0,
            "attachment_required": True,
            "unavailable_attachment_count": 1,
            "error": "artifact_unavailable",
            "created_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    calls: list[dict[str, object]] = []

    def fake_receipts(*, limit=20, status=None, path=None):
        calls.append({"limit": limit, "status": status, "path": path})
        return receipts

    monkeypatch.setattr(web_app, "list_platform_delivery_receipts", fake_receipts)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).get("/api/deliveries?status=failed&limit=5")

    assert response.status_code == 200
    assert response.json()["deliveries"] == receipts
    assert calls[-1]["status"] == "failed"
    assert calls[-1]["limit"] == 5
    assert TestClient(app).get("/api/deliveries?status=nope").status_code == 400
    assert 'id="deliveries-list"' in _HTML
    assert "function renderDeliveryReceipts(items)" in _HTML
    assert "delivery_health" in inspect.getsource(create_app)


def test_web_config_save_uses_langgraph_workflow() -> None:
    source = inspect.getsource(create_app)

    assert "class _WebConfigSaveState" in source
    assert "_compiled_web_config_save_graph" in source
    assert "StateGraph(_WebConfigSaveState)" in source
    assert "class _WebConfigModelTestState" in source
    assert "_compiled_web_config_model_test_graph" in source
    assert "StateGraph(_WebConfigModelTestState)" in source


def test_approval_card_javascript_rejects_internal_placeholder_details() -> None:
    assert "const placeholders = new Set([" in _HTML
    assert "'approval required'" in _HTML
    assert "Command details were not provided by the runtime." in _HTML


def test_web_allow_all_copy_is_unambiguous_about_domains() -> None:
    assert "Allow all web domains" in _HTML
    assert "Allow requests to any web domain for this scope" in _HTML
    assert "All web domains" in _HTML
    assert "Allow globally" not in _HTML
    assert "Global allow" not in _HTML


def test_connection_form_rejects_env_var_names_in_base_url_field() -> None:
    assert "Connector base URL must start with http:// or https://." in _HTML
    assert "Put env var names in the credential reference field." in _HTML
    assert 'env_name.endswith("_BASE_URL")' in inspect.getsource(create_app)
    assert "must be an http:// or https:// URL" in inspect.getsource(create_app)


def test_users_tab_exposes_member_edit_and_remove_controls() -> None:
    assert "Workspace ID" in _HTML
    assert "Messaging identity" in _HTML
    assert "function setUserWorkspaceId(index, value)" in _HTML
    assert "function removeUserMember(index)" in _HTML
    assert "Member removed. Save changes to apply." in _HTML


def test_connections_provider_dropdown_uses_skill_auth_metadata() -> None:
    assert "Only auth-required skills appear here." in _HTML
    assert "let skillAuthProviders = [];" in _HTML
    assert "renderConnectionProviderOptions(cfg.installed_skill_packs, cfg.skill_auth_providers);" in _HTML
    assert "function registerSkillAuthProviders(authProviders)" in _HTML
    assert "const uniqueProviders = [];" in _HTML
    assert "No auth-required skills installed" in _HTML
    assert '<option value="maton_connector_provider">' not in _HTML
    assert "DEFAULT_CONNECTION_PROVIDER_IDS" not in _HTML


def test_connections_ui_requires_confirmation_for_shared_admin_credentials() -> None:
    assert 'id="new-connection-scope"' in _HTML
    assert "Admin shared across workspaces" in _HTML
    assert "Share this admin credential?" in _HTML
    assert "workspaceId = 'workspace_admin';" in _HTML
    assert "credential_scope: credentialScope" in _HTML
    assert "Shared by admin" in _HTML
    assert "confirmAction({" in _HTML


def test_connections_ui_allows_changing_connector_permissions() -> None:
    assert 'id="new-connection-permission-mode"' in _HTML
    assert "Read-only requests" in _HTML
    assert "Read + write requests" in _HTML
    assert "permission_mode: permissionEl && permissionEl.value === 'write' ? 'write' : 'read'" in _HTML
    assert "async function setWorkspaceConnectionPermission(index, mode)" in _HTML
    assert "Allow connector writes?" in _HTML
    assert "window.confirm" not in _HTML


def test_web_restart_sends_gateway_restart_notice_before_exiting(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import desktop_entrypoint, gateway_notifications, web_app
    from nullion.tools import ToolRegistry

    calls: list[dict[str, object]] = []

    class Event:
        def to_dict(self):
            return {
                "event_id": "restart-1",
                "kind": "restarting",
                "text": "Nullion gateway is restarting",
                "created_at": "2026-04-30T12:00:00Z",
            }

    def fake_begin_gateway_restart(**kwargs):
        calls.append(kwargs)
        return Event()

    monkeypatch.setattr(gateway_notifications, "begin_gateway_restart", fake_begin_gateway_restart)
    monkeypatch.setattr(desktop_entrypoint, "schedule_desktop_reload", lambda **kwargs: None)
    monkeypatch.setattr(web_app, "_restart_non_web_services", lambda: "chat restarted")
    monkeypatch.setattr(web_app, "_schedule_process_restart", lambda: None)

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/restart")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["gateway_event"]["kind"] == "restarting"
    assert calls and calls[0]["async_delivery"] is False


def test_chat_services_restart_sends_gateway_restart_notice(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion import gateway_notifications, web_app
    from nullion.tools import ToolRegistry

    calls: list[dict[str, object]] = []

    class Event:
        def to_dict(self):
            return {
                "event_id": "chat-restart-1",
                "kind": "restarting",
                "text": "Nullion gateway is restarting",
                "created_at": "2026-04-30T12:00:00Z",
            }

    def fake_begin_gateway_restart(**kwargs):
        calls.append(kwargs)
        return Event()

    monkeypatch.setattr(gateway_notifications, "begin_gateway_restart", fake_begin_gateway_restart)
    monkeypatch.setattr(web_app, "_restart_chat_services", lambda **kwargs: "chat restarted")
    monkeypatch.setattr(web_app, "_chat_services_status_payload", lambda: [])

    app = create_app(SimpleNamespace(store=None), orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/chat-services/restart")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert calls and calls[0]["async_delivery"] is False


def test_update_restart_shows_restart_notice_without_waiting_for_websocket() -> None:
    assert "function showGatewayNotice(event)" in _HTML
    assert "shownGatewayEventIds.has(eventId)" in _HTML
    assert "localStorage.setItem('nullion_gateway_event_id', eventId);" in _HTML
    assert "} else if (data.type === 'gateway_notice') {\n        showGatewayNotice(data);" in _HTML
    assert "if (payload.gateway_event) showGatewayNotice(payload.gateway_event);" in _HTML
    local_notice = "showGatewayNotice({kind: 'restarting', text: '🟡 Nulliøn gateway is restarting. Chat may pause for a moment.'});"
    assert local_notice in _HTML
    assert _HTML.index(local_notice) < _HTML.index("const response = await fetch('/api/restart', { method: 'POST' });")


def test_settings_close_warns_about_unsaved_changes() -> None:
    assert "function settingsStateSnapshot()" in _HTML
    assert "function settingsHaveUnsavedChanges()" in _HTML
    assert "async function closeSettings()" in _HTML
    assert "Discard unsaved settings?" in _HTML
    assert "You have unsaved settings changes. Close Settings and lose those changes?" in _HTML
    assert "confirmText: 'Discard changes'" in _HTML
    assert "cancelText: 'Keep editing'" in _HTML
    assert "if (!discard) return;" in _HTML


def test_settings_dirty_state_is_tracked_and_reset_after_save() -> None:
    assert "let settingsBaselineSnapshot = '';" in _HTML
    assert "let settingsDirtyExplicit = false;" in _HTML
    assert "let settingsSnapshotReady = false;" in _HTML
    assert "refreshSettingsBaseline();" in _HTML
    assert "await refreshSettingsBaseline({force: true});" in _HTML
    assert "addEventListener('input', markSettingsDirty, true)" in _HTML
    assert "addEventListener('change', markSettingsDirty, true)" in _HTML
    assert "event.target.closest('.pref-chip')" in _HTML
    assert "markSettingsDirty();" in _HTML


def test_doctor_diagnose_button_posts_and_refreshes_existing_status_surfaces() -> None:
    assert "async function runDoctorDiagnose()" in _HTML
    assert "fetch('/api/doctor/diagnose', { method: 'POST' })" in _HTML
    assert "await loadBuilderDoctorSettingsSummary();" in _HTML
    assert "await refreshDashboard();" in _HTML
    assert "pollStatus" not in _HTML
    assert "API('/api/doctor/diagnose', { method: 'POST' })" not in _HTML


def test_web_verbose_mode_replaces_separate_verbose_and_planner_controls() -> None:
    assert 'id="verbose-mode-btn"' in _HTML
    assert 'id="verbose-mode-label">Verbose: Full</span>' in _HTML
    assert 'function cycleVerboseMode()' in _HTML
    assert "const order = ['off', 'planner', 'full'];" in _HTML
    assert 'id="cfg-verbose-mode"' in _HTML
    assert "verboseConfigForMode(document.getElementById('cfg-verbose-mode').value).activity_trace" in _HTML
    assert "verboseConfigForMode(document.getElementById('cfg-verbose-mode').value).task_planner_feed_mode" in _HTML
    assert 'id="planner-feed-btn"' not in _HTML
    assert 'id="cfg-task-planner-feed-mode"' not in _HTML
    assert 'id="activity-trace-toggle"' not in _HTML


def test_web_mini_agent_final_delivery_broadcasts_with_task_planner_mode(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.tools import ToolRegistry

    class CapturingOrchestrator:
        deliver_fn = None

        def set_deliver_fn(self, deliver_fn):
            self.deliver_fn = deliver_fn

    class FakeChatStore:
        def __init__(self) -> None:
            self.saved_messages = []

        def save_message(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.saved_messages.append((args, kwargs))
            return len(self.saved_messages)

    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "task")
    fake_store = FakeChatStore()
    monkeypatch.setattr("nullion.chat_store.get_chat_store", lambda: fake_store)
    orchestrator = CapturingOrchestrator()
    app = create_app(SimpleNamespace(store=None), orchestrator=orchestrator, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            assert orchestrator.deliver_fn is not None
            orchestrator.deliver_fn("web:operator", "Final delegated answer")

            data = websocket.receive_json()

    assert data["type"] == "background_message"
    assert data["conversation_id"] == "web:operator"
    assert data["text"] == "Final delegated answer"
    assert fake_store.saved_messages == [
        (("web:operator", "bot", "Final delegated answer"), {"is_error": False})
    ]


def test_task_status_cards_use_structured_checklist_renderer() -> None:
    assert "function renderTaskStatusText(text)" in _HTML
    assert "bubble.innerHTML = renderTaskStatusText(text);" in _HTML
    assert ".task-status-row {\n    display: grid; grid-template-columns: 14px minmax(0, 1fr); gap: 8px; align-items: start;\n    margin-left: 18px;" in _HTML
    assert ".task-status-icon {\n    width: 12px; height: 12px;" in _HTML
    assert "const states = {" in _HTML
    assert "'☐': {cls: 'pending', symbol: '☐'}" in _HTML
    assert "'▣': {cls: 'waiting', symbol: '▣'}" in _HTML
    assert "symbol: '!'" not in _HTML
    assert "symbol: ''" not in _HTML
    assert "'☑': {cls: 'complete', symbol: '☑'}" in _HTML


def test_task_status_cards_use_compact_trace_typography() -> None:
    assert ".task-status-card {\n    display: flex; flex-direction: column; gap: 6px;\n    font: 12px/1.35 -apple-system" in _HTML
    assert "task-status-title-icon" in _HTML
    assert "color: var(--green); font-size: 12px" in _HTML
    assert "color: var(--text); font-size: 12px; font-weight: 600; line-height: 1.35;" in _HTML
    assert "const titleIcon = allTerminal ? '✓' : '◐';" in _HTML
    assert ".task-status-list {\n    display: flex; flex-direction: column; gap: 2px;" in _HTML
    assert "color: var(--muted); font-size: 11px; line-height: 1.35;" in _HTML
    assert ".task-status-icon {\n    width: 12px; height: 12px;" in _HTML
    assert "'✕': {cls: 'failed', symbol: '✕'}" in _HTML
    assert "'◐': {cls: 'running', symbol: '◐'}" in _HTML


def test_settings_expose_mini_agent_iteration_and_continuation_limits() -> None:
    source = inspect.getsource(create_app)

    assert 'id="cfg-mini-agent-max-iterations"' in _HTML
    assert 'id="cfg-mini-agent-max-continuations"' in _HTML
    assert "cfg.mini_agent_max_continuations ?? 1" in _HTML
    assert "mini_agent_max_continuations: Number" in _HTML
    assert '"mini_agent_max_continuations": mini_agent_continuations' in source
    assert '("mini_agent_max_continuations", "NULLION_MINI_AGENT_MAX_CONTINUATIONS", 0)' in source


def test_set_default_chat_model_action_lives_with_chat_model_default_strip() -> None:
    button = '<button class="card-btn secondary" type="button" onclick="forceModelToAllSessions()">↗ Set as default for all sessions</button>'
    default_label = '<span class="ams-label">Default chat model:</span>'
    provider_test = '<div class="model-test-row provider-test-row">'

    assert _HTML.count(button) == 1
    assert _HTML.index(button) < _HTML.index(default_label)
    assert _HTML.index(button) > _HTML.index('id="cfg-model-name"')
    assert button not in _HTML[_HTML.index(provider_test):]


def test_web_approval_card_uses_explicit_web_flag_for_id_only_details() -> None:
    assert "Boolean(data.is_web_request)" in _HTML
    assert "Boolean(msg.is_web_request)" in _HTML
    assert "Boolean(resume.is_web_request)" in _HTML
    assert "function approvalActionsHtml(approvalId, toolName, detail, sessionAllowLabel = 'for all workspaces', forceWeb = false)" in _HTML
    assert "const isWeb = forceWeb || approvalIsWebRequest(toolName, detail);" in _HTML
    assert "haystack.includes('fetch a web page')" in _HTML
    assert "haystack.includes('allow web access')" in _HTML


def test_web_approval_display_payload_marks_web_tool_results_even_without_url_detail() -> None:
    class Runtime:
        store = None

    class Result:
        approval_id = "f9da193f-0000-0000-0000-000000000000"
        tool_results = []

    from nullion.tools import ToolResult

    Result.tool_results = [
        ToolResult(
            invocation_id="inv-web",
            tool_name="web_fetch",
            status="denied",
            output={
                "reason": "approval_required",
                "boundary_kind": "outbound_network",
                "target": "https://www.google.com/",
            },
        )
    ]

    label, detail, trigger, is_web_request = _approval_display_from_turn_result(Runtime(), Result())

    assert label == "fetch a web page"
    assert detail == "https://www.google.com/"
    assert trigger is None
    assert is_web_request is True


def test_web_approval_cards_show_tool_context_and_stale_clicks_do_not_say_denied() -> None:
    assert "Tool: ${escHtml(tool)}" in _HTML
    assert "data.stale" in _HTML
    assert "Approval expired" in _HTML
    assert "That approval is no longer pending. I refreshed approvals." in _HTML


def test_task_cards_expose_kill_controls_and_doctor_cleanup_copy() -> None:
    assert "async function killTask(kind, id, title)" in _HTML
    assert "/api/tasks/frame/${encodeURIComponent(id)}/kill" in _HTML
    assert "/api/tasks/mini-agent/${encodeURIComponent(id)}/kill" in _HTML
    assert "Stop this task and clear it from active work." in _HTML
    assert ".task-card {\n    display: grid;" in _HTML
    assert "function formatElapsedSince(raw, now = Date.now())" in _HTML
    assert 'class="task-elapsed" data-elapsed-since=' in _HTML
    assert "setInterval(updateElapsedCounters, 1000);" in _HTML
    assert "taskCardMetaHtml(t)" in _HTML
    assert '<div class="task-actions"><button class="mini-btn danger"' in _HTML
    assert '<div class="control-actions"><button class="mini-btn danger" title="Stop this task and clear it from active work."' not in _HTML
    assert "cleanup_dead_task_frame" in _HTML
    assert "Doctor cleared the stale task frame" in _HTML


def test_task_frame_kill_endpoint_cancels_active_frame(tmp_path) -> None:
    from datetime import UTC, datetime

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
    )
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    frame = TaskFrame(
        frame_id="frame-kill",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="dead task",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:operator", "frame-kill")

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    response = TestClient(app).post("/api/tasks/frame/frame-kill/kill")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert runtime.store.get_task_frame("frame-kill").status is TaskFrameStatus.CANCELLED
    assert runtime.store.get_active_task_frame_id("web:operator") is None


def test_mini_agent_kill_endpoint_delegates_to_orchestrator(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Orchestrator:
        def __init__(self) -> None:
            self.cancelled: list[str] = []

        async def cancel_task(self, task_id: str) -> bool:
            self.cancelled.append(task_id)
            return task_id == "task-1"

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    orchestrator = Orchestrator()
    app = create_app(runtime, orchestrator=orchestrator, registry=ToolRegistry())

    response = TestClient(app).post("/api/tasks/mini-agent/task-1/kill")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert orchestrator.cancelled == ["task-1"]


def test_status_refresh_cleans_dead_waiting_approval_frame_into_doctor_history(tmp_path) -> None:
    from datetime import UTC, datetime, timedelta

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.task_frames import (
        TaskFrame,
        TaskFrameExecutionContract,
        TaskFrameFinishCriteria,
        TaskFrameOperation,
        TaskFrameOutputContract,
        TaskFrameStatus,
    )
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    frame = TaskFrame(
        frame_id="frame-dead",
        conversation_id="web:operator",
        branch_id="branch",
        source_turn_id="turn",
        parent_frame_id=None,
        status=TaskFrameStatus.WAITING_APPROVAL,
        operation=TaskFrameOperation.ANSWER_WITH_CONTEXT,
        target=None,
        execution=TaskFrameExecutionContract(),
        output=TaskFrameOutputContract(),
        finish=TaskFrameFinishCriteria(),
        summary="orphaned approval wait",
        created_at=datetime.now(UTC) - timedelta(minutes=5),
        updated_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    runtime.store.add_task_frame(frame)
    runtime.store.set_active_task_frame_id("web:operator", frame.frame_id)

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    data = TestClient(app).get("/api/status").json()

    assert runtime.store.get_task_frame("frame-dead").status is TaskFrameStatus.CANCELLED
    assert runtime.store.get_active_task_frame_id("web:operator") is None
    assert any(
        item["recommendation_code"] == "cleanup_dead_task_frame"
        and item["status"] == "completed"
        and "orphaned approval wait" in item["summary"]
        for item in data["doctor_actions"]
    )


def test_doctor_diagnose_endpoint_returns_report(tmp_path) -> None:
    from types import SimpleNamespace

    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    class Orchestrator:
        def get_status(self):
            return [SimpleNamespace(task_id="mini-live")]

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    app = create_app(runtime, orchestrator=Orchestrator(), registry=ToolRegistry())

    response = TestClient(app).post("/api/doctor/diagnose")
    data = response.json()

    assert response.status_code == 200
    assert data["ok"] is True
    assert isinstance(data["report"], dict)
    assert data["report"]["summary"]


def test_status_refresh_does_not_replace_store_while_web_turn_is_active(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from nullion.approvals import create_approval_request
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.checkpoint()
    approval = create_approval_request(
        requested_by="web:test",
        action="allow_boundary",
        resource="https://example.com/*",
        request_kind="boundary_policy",
    )
    runtime.store.add_approval_request(approval)
    runtime.last_checkpoint_fingerprint = "force-refresh"

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry())
    client = TestClient(app)

    with app.state.nullion_runtime_turn_guard():
        response = client.get("/api/status")

    assert response.status_code == 200
    assert runtime.store.get_approval_request(approval.approval_id) is not None

    client.get("/api/status")
    assert runtime.store.get_approval_request(approval.approval_id) is None


def test_model_provider_test_controls_live_at_bottom_of_provider_panel() -> None:
    media_list_pos = _HTML.index('<div id="media-model-list" class="media-model-list"></div>')
    test_button_pos = _HTML.index('id="model-test-btn"')
    provider_body_end = _HTML.index('</div>\n      </div>\n\n      <!-- Setup tab -->')

    assert media_list_pos < test_button_pos < provider_body_end
    assert "provider-test-row" in _HTML


def test_web_chat_allows_parallel_active_turns() -> None:
    assert "let _activeSendTurnIds = new Set();" in _HTML
    assert "let _activeTurnTimers = new Map();" in _HTML
    assert "function chatTurnInFlight()" in _HTML
    assert "function beginTurnUi(turnId, text)" in _HTML
    assert "function finishTurnUi(turnId = null)" in _HTML
    assert "_activeSendTurnIds.add(id);" in _HTML
    assert "_activeSendTurnIds.delete(turnId);" in _HTML
    assert "async function sendMessage() {\n  if (chatTurnInFlight())" not in _HTML
    assert "sendMessage = async function() {\n  if (chatTurnInFlight())" not in _HTML
    assert "beginTurnUi(turnId, displayText);" in _HTML
    assert "finishTurnUi(data.turn_id || null);" in _HTML


def test_websocket_chat_dispatches_turns_concurrently() -> None:
    source = Path(web_app.__file__).read_text(encoding="utf-8")
    assert "active_turn_tasks: set[asyncio.Task] = set()" in source
    assert "task = asyncio.create_task(process_chat_payload(payload, dependency_tasks))" in source
    assert "send_lock = asyncio.Lock()" in source
    assert "route_turn_dispatch(" in source


def test_websocket_chat_can_finish_later_message_first(tmp_path, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry

    def fake_run_turn_sync(user_text, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if "slow" in str(user_text):
            time.sleep(0.2)
        return {"text": f"{user_text} done", "artifacts": []}

    monkeypatch.setattr(web_app, "_run_turn_sync", fake_run_turn_sync)
    app = create_app(bootstrap_persistent_runtime(tmp_path / "runtime.db"), orchestrator=None, registry=ToolRegistry())

    with TestClient(app) as client:
        with client.websocket_connect("/ws/chat") as websocket:
            websocket.send_json({"text": "slow", "conversation_id": "web:test", "turn_id": "turn:slow", "stream": False})
            websocket.send_json({"text": "fast", "conversation_id": "web:test", "turn_id": "turn:fast", "stream": False})

            first = websocket.receive_json()
            assert first == {"turn_id": "turn:fast", "type": "chunk", "text": "fast done"}

            seen_done = {websocket.receive_json()["turn_id"]}
            while seen_done != {"turn:fast", "turn:slow"}:
                event = websocket.receive_json()
                if event["type"] == "done":
                    seen_done.add(event["turn_id"])


def test_finalize_bot_message_does_not_reenable_send_button() -> None:
    start = _HTML.index("function finalizeBotMsg(")
    end = _HTML.index("function approvalToolIcon(", start)
    body = _HTML[start:end]

    assert "disabled = false" not in body
    assert "setSendButtonDisabled(false)" not in body
    assert "finishTurnUi(" not in body


def test_approval_resume_updates_original_activity_trace() -> None:
    assert "let _activityElByApproval = new Map();" in _HTML
    assert "function rememberApprovalActivity(approvalId, turnId = null)" in _HTML
    assert "function updateApprovalRunActivity(approvalId, event)" in _HTML
    assert "rememberApprovalActivity(data.approval_id, data.turn_id || null);" in _HTML
    assert "rememberApprovalActivity(msg.approval_id, turnId);" in _HTML
    assert "handleApprovalResume(data, approvalId);" in _HTML
    assert "activity.forEach(event => updateApprovalRunActivity(activityApprovalId, event));" in _HTML


def test_model_provider_test_uses_refreshed_csrf_and_only_connection_rows() -> None:
    assert "async function refreshLocalCsrfToken()" in _HTML
    assert "const res = await nativeFetch('/api/session'" in _HTML
    assert "return nativeFetch(input, {...nextInit, headers: retryHeaders});" in _HTML
    assert "id=\"mtr-media-" not in _HTML
    assert "Enter at least one chat model for the selected provider." in _HTML


def test_media_model_normalization_dedupes_and_drops_invalid_records() -> None:
    assert _normalize_media_models(
        {
            "openai": [
                {"model": "gpt-4o", "capabilities": ["image_input", "image_input"]},
                {"model": "gpt-4o", "capabilities": ["audio_input"]},
                {"name": "gpt-image-1", "capabilities": ["image_output"]},
                {"capabilities": ["image_input"]},
            ],
            "": [{"model": "ignored"}],
            "gemini": "not-a-list",
        }
    ) == {
        "openai": [
            {"model": "gpt-4o", "capabilities": ["image_input"]},
            {"model": "gpt-image-1", "capabilities": ["image_output"]},
        ]
    }


def test_invalid_media_model_capabilities_are_reported_before_save() -> None:
    errors = _invalid_media_model_capabilities(
        {
            "codex": [
                {"model": "gpt-5.5", "capabilities": ["audio_input"]},
                {"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]},
            ],
            "openai": [{"model": "gpt-4o", "capabilities": []}],
        }
    )

    assert "codex · gpt-5.5 does not look valid for audio_input" in errors
    assert "codex · gemini-3.1-flash-image-preview does not look valid for image_output" in errors
    assert "openai · gpt-4o needs a model type" in errors


def test_media_models_are_provider_scoped() -> None:
    assert _media_model_supports("image_generate", "codex", "gemini-3.1-flash-image-preview") is False
    assert _media_model_supports("image_generate", "gemini", "gemini-3.1-flash-image-preview") is True
    assert _filter_supported_media_models(
        {
            "codex": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}],
            "gemini": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}],
        }
    ) == {
        "gemini": [{"model": "gemini-3.1-flash-image-preview", "capabilities": ["image_output"]}]
    }


def test_media_model_javascript_rejects_provider_mismatches() -> None:
    assert "function mediaModelMatchesProvider(provider, model)" in _HTML
    assert "belongs under a different provider" in _HTML


def test_media_selection_rejects_partial_selection_and_codex_audio_even_if_declared() -> None:
    media_models = {"codex": [{"model": "gpt-5.5", "capabilities": ["audio_input", "image_input"]}]}

    assert _media_selection_supported("image_ocr", provider="", model="", media_models=media_models) is True
    assert _media_selection_supported("image_ocr", provider="codex", model="", media_models=media_models) is False
    assert _media_selection_supported("audio_transcribe", provider="codex", model="gpt-5.5", media_models=media_models) is False
    assert _media_selection_supported("image_ocr", provider="codex", model="gpt-5.5", media_models=media_models) is True
