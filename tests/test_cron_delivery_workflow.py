from __future__ import annotations

import pytest

import json
import inspect
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from nullion.cron_delivery import (
    CronRunDeliveryCallbacks,
    DEFAULT_CRON_NO_OUTPUT_MESSAGE,
    configured_delivery_target,
    cron_artifact_validation_block_reason,
    cron_delivery_artifact_paths_from_result,
    cron_agent_history,
    cron_agent_prompt,
    cron_delivery_block_reason,
    cron_delivery_reply_text,
    cron_conversation_id,
    cron_delivery_target,
    cron_delivery_text,
    cron_delivery_text_from_result,
    cron_structured_result_block_reason,
    cancel_manual_cron_background_runs,
    default_cron_agent_max_iterations,
    effective_cron_delivery_channel,
    manual_cron_silent_delivery_text,
    manual_cron_deferred_receipt,
    manual_cron_running_status_text,
    manual_cron_terminal_status_text,
    normalize_cron_delivery_channel,
    record_interrupted_cron_delivery_runs,
    run_cron_delivery_workflow,
    run_single_agent_cron_turn,
    scheduled_task_delivery_text,
    start_manual_cron_background_delivery,
    _filter_cron_delivery_media_to_required_extensions,
    _cron_structured_html_report,
    _apply_cron_alert_delivery_policy,
    _recover_typed_html_artifact_after_terminal_limit,
    _repair_cron_agent_final_text_if_needed,
)
from nullion.artifact_validation import missing_required_artifact_content_tokens
from nullion.platform_activity import PlatformTaskCardStore, platform_activity_capabilities
from nullion.response_sanitizer import safe_raw_tool_payload_replacement, sanitize_user_visible_reply
from nullion.tools import ToolRegistry, ToolResult


@dataclass
class Job:
    id: str = "cron-1"
    name: str = "Morning brief"
    task: str = "make the brief"
    delivery_channel: str = ""
    delivery_target: str = ""
    html_image_delivery_mode: str = ""
    artifact_delivery_options: dict[str, object] | None = None


class EventStore:
    def __init__(self) -> None:
        self.events: list[object] = []

    def list_events(self) -> list[object]:
        return list(self.events)

    def add_event(self, event: object) -> None:
        self.events.append(event)


def test_web_save_failure_does_not_record_a_successful_delivery():
    events=[]; receipts=[]
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'web',delivery_target=lambda j,c:'web:user',
        run_agent_turn=lambda j,c:{'text':'Checked facts','response_fulfilled':True},
        record_event=lambda kind,*a,**k:events.append(kind),block_reason=lambda *a:None,
        save_web_delivery=lambda *a:False,send_platform_delivery=lambda *a:True,
        record_chat_turn=lambda *a:receipts.append(a))
    result=run_cron_delivery_workflow(Job(),label='Scheduled task',callbacks=callbacks)
    assert result['cron_delivery_status']=='failed' and result['cron_delivery_failed']
    assert events==['cron.delivery.started','cron.delivery.failed'] and not receipts
    assert result['reason']=='web_delivery_failed'


def test_transport_exception_uses_bounded_retry_and_records_failure():
    events=[]; calls=[]
    def transport(*args):
        calls.append(args)
        raise ConnectionError('External transport offline')
    callbacks=CronRunDeliveryCallbacks(effective_channel=lambda j:'telegram',delivery_target=lambda j,c:'123',
        run_agent_turn=lambda j,c:{'text':'Checked facts','response_fulfilled':True},
        record_event=lambda kind,*a,**k:events.append(kind),block_reason=lambda *a:None,
        save_web_delivery=lambda *a:False,send_platform_delivery=transport)
    result=run_cron_delivery_workflow(Job(),label='Scheduled task',callbacks=callbacks)
    assert len(calls)==2 and result['cron_delivery_status']=='failed'
    assert 'cron.delivery.retry' in events and 'cron.delivery.sent' not in events


def test_record_interrupted_cron_delivery_runs_marks_latest_active_runs_failed() -> None:
    now = datetime(2026, 5, 28, 21, 0, tzinfo=UTC)
    store = EventStore()
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.agent_preflight",
            created_at=now - timedelta(minutes=5),
            payload={"cron_id": "running-cron", "cron_name": "Running cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.started",
            created_at=now - timedelta(minutes=4),
            payload={"cron_id": "sent-cron", "cron_name": "Sent cron", "delivery_channel": "telegram"},
        )
    )
    store.add_event(
        SimpleNamespace(
            event_type="cron.delivery.sent",
            created_at=now - timedelta(minutes=3),
            payload={"cron_id": "sent-cron", "cron_name": "Sent cron", "delivery_channel": "telegram"},
        )
    )

    recorded = record_interrupted_cron_delivery_runs(store, actor="telegram_startup", now=now)

    assert recorded == 1
    failure = store.events[-1]
    assert failure.event_type == "cron.delivery.failed"
    assert failure.actor == "telegram_startup"
    assert failure.payload["cron_id"] == "running-cron"
    assert failure.payload["reason"] == "cron_run_interrupted_by_runtime_restart"
    assert failure.payload["previous_event_type"] == "cron.delivery.agent_preflight"


def test_legacy_cron_routes_to_telegram_when_operator_target_exists() -> None:
    job = Job()
    env = {"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "8675309"}

    channel = effective_cron_delivery_channel(job, env=env)
    target = cron_delivery_target(job, channel, env=env)

    assert channel == "telegram"
    assert target == "8675309"
    assert cron_conversation_id(job, channel, target) == "telegram:8675309"


def test_explicit_web_cron_stays_web_even_when_telegram_is_configured() -> None:
    job = Job(delivery_channel="web")
    env = {"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "8675309"}

    channel = effective_cron_delivery_channel(job, env=env)

    assert channel == "web"
    assert cron_delivery_target(job, channel, env=env) == "web:operator"
    assert cron_conversation_id(job, channel, "web:operator") == "web:operator"


def test_messaging_cron_ignores_stale_web_target_and_uses_configured_chat() -> None:
    job = Job(delivery_channel="telegram", delivery_target="web:operator")
    env = {"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "12345"}

    assert cron_delivery_target(job, "telegram", env=env) == "12345"


def test_workspace_messaging_target_beats_ambient_operator_env(monkeypatch) -> None:
    from nullion import users

    job = SimpleNamespace(
        delivery_channel="telegram",
        delivery_target="",
        workspace_id="workspace_reminders",
    )
    workspace_targets = (
        users.MessagingDeliveryTarget(
            channel="telegram",
            target_id="workspace-member-telegram",
            principal_id="user:member-reminder",
            workspace_id="workspace_reminders",
            user_id="member-reminder",
            display_name="Workspace Member",
        ),
    )
    monkeypatch.setattr(
        users,
        "messaging_delivery_targets_for_workspace",
        lambda workspace_id, *, settings=None: workspace_targets,
    )

    assert (
        cron_delivery_target(job, "telegram", env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "789"})
        == "workspace-member-telegram"
    )


def test_slack_and_discord_targets_resolve_from_configured_environment() -> None:
    env = {
        "NULLION_SLACK_OPERATOR_USER_ID": "U123",
        "NULLION_DISCORD_OPERATOR_CHANNEL_ID": "C456",
    }

    assert configured_delivery_target("slack", env=env) == "U123"
    assert configured_delivery_target("discord", env=env) == "C456"
    assert cron_conversation_id(Job(id="slack-cron"), "slack", "U123") == "slack:U123"
    assert cron_conversation_id(Job(id="discord-cron"), "discord", "C456") == "discord:C456"


def test_unknown_delivery_channel_falls_back_to_cron_conversation_id() -> None:
    assert cron_conversation_id(Job(id="cron-9"), "unknown", "") == "cron:cron-9"


def test_cron_delivery_text_does_not_attach_internal_artifacts() -> None:
    artifact = Path("/tmp/nullion-artifact-report.html")

    assert cron_delivery_text("Done", [{"path": str(artifact)}, {"path": str(artifact)}]) == "Done"
    assert cron_delivery_text(f"Done\nMEDIA:{artifact}", [{"path": str(artifact)}]) == f"Done\nMEDIA:{artifact}"


def test_cron_delivery_text_never_replaces_final_reply_with_text_artifact_content(tmp_path) -> None:
    artifact = tmp_path / "attachment_ocr.txt"
    artifact.write_text("NOTICE DATE\nPRIVATE DOCUMENT BODY", encoding="utf-8")

    delivered = cron_delivery_text(
        "No new deadline was verified from the reviewed messages.",
        [{"path": str(artifact)}],
    )

    assert delivered == "No new deadline was verified from the reviewed messages."
    assert "NOTICE DATE" not in delivered
    assert "PRIVATE DOCUMENT BODY" not in delivered


def test_unknown_cron_channel_normalizes_to_blank() -> None:
    assert normalize_cron_delivery_channel("sms") == ""


def test_cron_agent_prompt_declares_file_text_silent_and_default_delivery_contract() -> None:
    prompt = cron_agent_prompt(Job(name="Repo watch", task="Check public repo state."), label="Scheduled task")

    assert "[Scheduled task: Repo watch] Check public repo state." in prompt
    assert "existing scheduled task run" in prompt
    assert "not a request to create another schedule" in prompt
    assert "Do not create, update, delete, toggle, or run scheduled tasks" in prompt
    assert "Cron may deliver text, file attachments, both, or no message" in prompt
    assert "attach it with a MEDIA line" in prompt
    assert "browser_connection_notice" in prompt
    assert "do not imply that authenticated browser state was available" in prompt
    assert "Shape the visible reply to the amount and type of content" in prompt
    assert "Do not force every reply into bullets" in prompt
    assert "return no output when nothing changed" in prompt
    assert DEFAULT_CRON_NO_OUTPUT_MESSAGE not in prompt
    assert "without source evidence" in prompt


def test_scheduled_task_delivery_text_uses_shared_visible_header() -> None:
    assert scheduled_task_delivery_text(Job(name="Morning brief"), "Done") == "⏰ **SCHEDULED TASK: Morning brief**\n\nDone"
    assert (
        scheduled_task_delivery_text(Job(name="Morning brief"), "Done", run_label="Manual scheduled task run")
        == "⏰ **MANUAL SCHEDULED TASK RUN: Morning brief**\n\nDone"
    )
    assert scheduled_task_delivery_text(Job(name=""), "") == "⏰ **SCHEDULED TASK: Scheduled task**"


def test_platform_attachment_delivery_preserves_scheduled_task_heading_block(tmp_path: Path, monkeypatch) -> None:
    from nullion import preferences
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(preferences, "load_preferences", lambda: SimpleNamespace(emoji_level="none"))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "random_web_stress_20260606.docx"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"PK\x03\x04docx")
    reply = (
        "⏰ **SCHEDULED TASK: Random website artifact generator**\n\n"
        "Random website artifact generator completed\n"
        "Websites used this run\n"
        f"MEDIA:{artifact}"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.text == (
        "⏰ **SCHEDULED TASK: Random website artifact generator**\n\n"
        "Random website artifact generator completed\n"
        "Websites used this run"
    )
    assert delivery.attachments == (artifact,)


def test_platform_attachment_delivery_repairs_plain_scheduled_task_heading(tmp_path: Path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "random_web_stress_20260606.docx"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"PK\x03\x04docx")
    reply = (
        "SCHEDULED TASK: Random website artifact generator\n"
        "Random website artifact generator completed\n"
        f"MEDIA:{artifact}"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.text == (
        "⏰ **SCHEDULED TASK: Random website artifact generator**\n\n"
        "Random website artifact generator completed"
    )
    assert delivery.attachments == (artifact,)


def test_scheduled_task_delivery_text_combines_report_clock_with_header() -> None:
    body = "⏰ Email Deadline Triage — 2026-05-21 08:02 AM\n\nNo deadlines found."

    assert scheduled_task_delivery_text(Job(name="Email deadline triage Maton Gmail"), body) == (
        "⏰ **SCHEDULED TASK: Email deadline triage Maton Gmail — 2026-05-21 08:02 AM**\n\n"
        "No deadlines found."
    )


def test_scheduled_task_delivery_text_collapses_source_line_before_report_clock() -> None:
    body = (
        "⏰ Maton Gmail\n\n"
        "⏰ 📬 Email deadline triage — 2026-05-22 01:00 PM\n\n"
        "No deadlines found."
    )

    assert scheduled_task_delivery_text(Job(name="Email deadline triage Maton Gmail"), body) == (
        "⏰ **SCHEDULED TASK: Email deadline triage Maton Gmail — 2026-05-22 01:00 PM**\n\n"
        "No deadlines found."
    )


def test_scheduled_task_delivery_text_removes_duplicate_clock_from_report_title() -> None:
    body = "⏰ ✉️ Peak Design shipment update: in transit\n📦 USPS tracking still in transit."

    assert scheduled_task_delivery_text(Job(name="Peak Design shipment tracker"), body) == (
        "⏰ **SCHEDULED TASK: Peak Design shipment tracker**\n\n"
        "✉️ Peak Design shipment update: in transit\n"
        "📦 USPS tracking still in transit."
    )


def test_scheduled_task_delivery_text_removes_duplicate_clock_from_simple_body() -> None:
    assert scheduled_task_delivery_text(Job(name="Morning brief"), "⏰ Done") == (
        "⏰ **SCHEDULED TASK: Morning brief**\n\n"
        "Done"
    )


def test_scheduled_task_delivery_text_formats_dense_list_for_messaging() -> None:
    body = (
        "Email deadline triage completed.\n\n"
        "Found/read relevant recent messages:\n"
        "• Katona / Docketwise I-751 matter — received Jun 1. Action needed: complete Docketwise intake form, "
        "upload/provide checklist documents, and pay initial legal fee of $2,000. No specific due date stated.\n"
        "1. Calendar already has consultation event.\n"
        "2. Skipped creating duplicate.\n"
        "• Betterment automatic deposit — received Jun 4. $250 transfer scheduled for Jun 5, 2026; skip deadline "
        "is 4:00 PM ET on Jun 4.\n"
        "3. Skipped creating duplicate.\n\n"
        "Other search hits reviewed as likely not obligations: receipts and confirmations."
    )

    delivered = scheduled_task_delivery_text(Job(name="Email deadline triage"), body)

    assert delivered.startswith("⏰ **SCHEDULED TASK: Email deadline triage**\n\n")
    assert "📌 **Found/read relevant recent messages**\n\n• Katona" in delivered
    assert "\n  1. Calendar already has consultation event." in delivered
    assert "\n\n• Betterment automatic deposit" in delivered
    assert "documents, and pay initial legal fee" not in delivered
    assert delivered.count("\n\n• ") == 2


def test_scheduled_task_delivery_text_keeps_simple_bullet_summary_compact() -> None:
    body = (
        "Weather report\n\n"
        "• 94°/63° — overcast\n\n"
        "• Rain outlook: 0% chance; dry.\n\n"
        "• Wear: light breathable clothes; sunglasses."
    )

    delivered = scheduled_task_delivery_text(Job(name="Daily weather"), body)

    assert delivered == (
        "⏰ **SCHEDULED TASK: Daily weather**\n\n"
        "📌 **Weather report**\n\n"
        "• 94°/63° — overcast\n"
        "• Rain outlook: 0% chance; dry.\n"
        "• Wear: light breathable clothes; sunglasses."
    )


def test_scheduled_task_delivery_text_keeps_numbered_details_attached_to_parent_item() -> None:
    body = (
        "Found records:\n"
        "• First record has a long enough body that the wrapping helper will keep it readable on messaging surfaces.\n\n"
        "1. Existing event found.\n"
        "2. No duplicate created.\n"
        "• Second record summary.\n"
        "3. No action needed."
    )

    delivered = scheduled_task_delivery_text(Job(name="Grouped records"), body)

    assert "• First record" in delivered
    assert "\n  1. Existing event found." in delivered
    assert "\n  2. No duplicate created." in delivered
    assert "\n\n• Second record summary." in delivered
    assert "\n  3. No action needed." in delivered


def test_scheduled_task_delivery_text_compacts_numbered_scalar_result_pairs() -> None:
    body = (
        "Results I found\n\n"
        "1. Critical Metals Corp\n\n"
        "$9.41\n\n"
        "2. Microsoft Corp\n\n"
        "$403.45\n\n"
        "3. TMC the metals company Inc\n\n"
        "$4.93\n\n"
        "4. Tempus AI Inc\n\n"
        "$50.95"
    )

    delivered = scheduled_task_delivery_text(Job(name="Mid-Market Stock Alert"), body)

    assert delivered == (
        "⏰ **SCHEDULED TASK: Mid-Market Stock Alert**\n\n"
        "📌 **Results I found**\n\n"
        "1. Critical Metals Corp — $9.41\n"
        "2. Microsoft Corp — $403.45\n"
        "3. TMC the metals company Inc — $4.93\n"
        "4. Tempus AI Inc — $50.95"
    )


def test_scheduled_task_delivery_text_compacts_heading_paragraph_groups_for_chat() -> None:
    body = (
        "Daily synthetic update:\n\n"
        "Primary movement:\n\n"
        "Jun 17, 6:00pm — Rooftop venue, Metro City\n\n"
        "Vendor A is now from $203, down from prior $221.\n\n"
        "Link: https://example.com/very/long/link/path/that/will/be-hard-on-mobile\n\n"
        "New broader region listing:\n\n"
        "Jun 22, 8:00pm — The Hall, Harbor Town\n\n"
        "Vendor B has a newly surfaced listing from $313; table lowest $312.\n\n"
        "Link: https://example.org/items/alpha-beta-gamma\n\n"
        "Other nearby references checked today:\n\n"
        "Jun 24 — Bridgeport remains the cheapest nearby deal: Vendor A from $28 / Vendor B from $30."
    )

    delivered = scheduled_task_delivery_text(Job(name="Synthetic watch"), body)

    assert delivered.startswith("⏰ **SCHEDULED TASK: Synthetic watch**\n\n")
    assert "📌 **Daily synthetic update**\n\n📌 **Primary movement**" in delivered
    assert "\n\n• Jun 17, 6:00pm — Rooftop venue, Metro City\n" in delivered
    assert "\n• Vendor A is now from $203, down from prior $221.\n" in delivered
    assert "\n• Link: https://example.com/very/long/link/path/that/will/be-hard-on-mobile\n" in delivered
    assert "\n\n📌 **New broader region listing**\n\n• Jun 22, 8:00pm" in delivered
    assert "\n\n📌 **Other nearby references checked today**\n\n• Jun 24" in delivered
    assert "Primary movement:" not in delivered
    assert "\n\nVendor A is now" not in delivered
    assert "\n\nLink: https://example.com" not in delivered


def test_scheduled_task_delivery_text_preserves_emoji_bold_heading_without_trailing_stars() -> None:
    delivered = scheduled_task_delivery_text(
        Job(name="Example Daily Weather"),
        "🌂 **Weather report**\n\n- **75°/61°** overcast\n- **Rain:** none expected",
    )

    assert "🌂 **Weather report**" in delivered
    assert "Weather report****" not in delivered
    assert "**🌂 **Weather report****" not in delivered


def test_cron_delivery_blocks_malformed_internal_delivery_text() -> None:
    leaked_text = (
        "]\n"
        "baqend-speedkit $1d1632e7-bc1d-4b69-b16d-1f09fc0da2f1 (0:\n"
        "b a q e n d - s p e e d k i t\n"
        "�\n\n"
        "baqend-speedkit-assets-default $966a0386-c140-486a-8d5c-2e99d5dca267 ���"
    )

    assert (
        cron_structured_result_block_reason(
            {
                "final_text": leaked_text,
                "tool_results": [ToolResult("browser", "browser_extract_text", "completed", {})],
            },
            artifacts=None,
            text=leaked_text,
        )
        == "cron_run_malformed_delivery_text"
    )


def test_cron_delivery_blocks_recursive_internal_preview_schema() -> None:
    leaked_text = '{"original_chars": 23750, "preview": "{\\"original_chars\\": 23746}"}'

    assert (
        cron_structured_result_block_reason(
            {
                "final_text": leaked_text,
                "tool_results": [ToolResult("list", "list_crons", "completed", {})],
            },
            artifacts=None,
            text=leaked_text,
        )
        == "cron_run_malformed_delivery_text"
    )


def test_cron_single_agent_runner_has_no_deep_agent_dispatch_path() -> None:
    from nullion import cron_delivery

    source = inspect.getsource(cron_delivery.run_single_agent_cron_turn)

    assert "dispatch_request_sync" not in source
    assert "cron_deep_agent_dispatch_plan" not in source
    assert "mini_agent_dispatch" not in source


def test_cron_planner_status_preview_omits_runtime_delivery_only_terminal_steps(monkeypatch) -> None:
    from nullion.cron_planner_status import build_cron_planner_status_preview

    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    captured: dict[str, object] = {}
    cache_buster = uuid4().hex

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            captured["messages"] = kwargs.get("messages")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "disposition": "sequential_mission",
                                "needs_clarification": False,
                                "clarification_question": None,
                                "routing_evidence": ["model_structured_plan"],
                                "tasks": [
                                    {
                                        "title": "データ収集",
                                        "description": "Collect verified connector data.",
                                        "tool_scope": ["connector_request"],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": "verified_data",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "確認内容の整理",
                                        "description": "Synthesize verified obligations.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [0],
                                        "context_key_in": "verified_data",
                                        "context_key_out": "verified_obligations",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "表示文を整える",
                                        "description": "Prepare the platform-facing text.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [1],
                                        "context_key_in": "verified_obligations",
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "通知先へ送る",
                                        "description": "Send via the configured platform.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [2],
                                        "context_key_in": None,
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                ],
                            }
                        ),
                    }
                ]
            }

    class FakeToolRegistry:
        def list_tool_definitions(self):
            return [{"name": "connector_request"}]

    preview = build_cron_planner_status_preview(
        model_client=FakeModelClient(),
        user_message=f"定期ジョブの確認を実行する。runtime_delivery_strip_unit_{cache_buster}",
        conversation_id="telegram:100001",
        principal_id="telegram:100001",
        tool_registry=FakeToolRegistry(),
        subject=f"定期確認 runtime_delivery_strip_unit_{cache_buster}",
    )

    assert preview is not None
    prompt_text = captured["messages"][0]["content"][0]["text"]
    assert "external_platform_delivery_managed_by_runtime=true" in prompt_text
    assert [task.title for task in preview.group.tasks] == ["データ収集", "確認内容の整理"]
    assert "表示文を整える" not in preview.initial_text()
    assert "通知先へ送る" not in preview.initial_text()


def test_scheduled_task_delivery_text_combines_report_clock_with_header() -> None:
    body = "⏰ Email Deadline Triage — 2026-05-21 08:02 AM\n\nNo deadlines found."

    assert scheduled_task_delivery_text(Job(name="Email deadline triage Maton Gmail"), body) == (
        "⏰ **SCHEDULED TASK: Email deadline triage Maton Gmail — 2026-05-21 08:02 AM**\n\n"
        "No deadlines found."
    )


def test_scheduled_task_delivery_text_collapses_source_line_before_report_clock() -> None:
    body = (
        "⏰ Maton Gmail\n\n"
        "⏰ 📬 Email deadline triage — 2026-05-22 01:00 PM\n\n"
        "No deadlines found."
    )

    assert scheduled_task_delivery_text(Job(name="Email deadline triage Maton Gmail"), body) == (
        "⏰ **SCHEDULED TASK: Email deadline triage Maton Gmail — 2026-05-22 01:00 PM**\n\n"
        "No deadlines found."
    )


def test_scheduled_task_delivery_text_removes_duplicate_clock_from_report_title() -> None:
    body = "⏰ ✉️ Peak Design shipment update: in transit\n📦 USPS tracking still in transit."

    assert scheduled_task_delivery_text(Job(name="Peak Design shipment tracker"), body) == (
        "⏰ **SCHEDULED TASK: Peak Design shipment tracker**\n\n"
        "✉️ Peak Design shipment update: in transit\n"
        "📦 USPS tracking still in transit."
    )


def test_scheduled_task_delivery_text_removes_duplicate_clock_from_simple_body() -> None:
    assert scheduled_task_delivery_text(Job(name="Morning brief"), "⏰ Done") == (
        "⏰ **SCHEDULED TASK: Morning brief**\n\n"
        "Done"
    )


def test_cron_planner_status_preview_omits_runtime_delivery_only_terminal_steps(monkeypatch) -> None:
    from nullion.cron_planner_status import build_cron_planner_status_preview

    monkeypatch.setenv("NULLION_CRON_PLANNER_MODEL_PREVIEW_ENABLED", "1")
    captured: dict[str, object] = {}
    cache_buster = uuid4().hex

    class FakeModelClient:
        def create(self, **kwargs):  # noqa: ANN003
            captured["messages"] = kwargs.get("messages")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "disposition": "sequential_mission",
                                "needs_clarification": False,
                                "clarification_question": None,
                                "routing_evidence": ["model_structured_plan"],
                                "tasks": [
                                    {
                                        "title": "データ収集",
                                        "description": "Collect verified connector data.",
                                        "tool_scope": ["connector_request"],
                                        "priority": "normal",
                                        "dependencies": [],
                                        "context_key_in": None,
                                        "context_key_out": "verified_data",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "確認内容の整理",
                                        "description": "Synthesize verified obligations.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [0],
                                        "context_key_in": "verified_data",
                                        "context_key_out": "verified_obligations",
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "表示文を整える",
                                        "description": "Prepare the platform-facing text.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [1],
                                        "context_key_in": "verified_obligations",
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                    {
                                        "title": "通知先へ送る",
                                        "description": "Send via the configured platform.",
                                        "tool_scope": [],
                                        "priority": "normal",
                                        "dependencies": [2],
                                        "context_key_in": None,
                                        "context_key_out": None,
                                        "required_inputs": [],
                                        "can_start": True,
                                    },
                                ],
                            }
                        ),
                    }
                ]
            }

    class FakeToolRegistry:
        def list_tool_definitions(self):
            return [{"name": "connector_request"}]

    preview = build_cron_planner_status_preview(
        model_client=FakeModelClient(),
        user_message=f"定期ジョブの確認を実行する。runtime_delivery_strip_unit_{cache_buster}",
        conversation_id="telegram:100001",
        principal_id="telegram:100001",
        tool_registry=FakeToolRegistry(),
        subject=f"定期確認 runtime_delivery_strip_unit_{cache_buster}",
    )

    assert preview is not None
    prompt_text = captured["messages"][0]["content"][0]["text"]
    assert "external_platform_delivery_managed_by_runtime=true" in prompt_text
    assert [task.title for task in preview.group.tasks] == ["データ収集", "確認内容の整理"]
    assert "表示文を整える" not in preview.initial_text()
    assert "通知先へ送る" not in preview.initial_text()


def test_cron_run_delivery_workflow_saves_web_result() -> None:
    events: list[str] = []
    saved: list[tuple[str, str]] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "web",
        delivery_target=lambda job, channel: "web:operator",
        run_agent_turn=lambda job, conv_id: {"text": "Done"},
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: saved.append((conv_id, text)) or True,
        send_platform_delivery=lambda job, channel, text: False,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="web"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "saved"
    assert saved == [("web:operator", "⏰ **SCHEDULED TASK: Morning brief**\n\nDone")]
    assert events == ["cron.delivery.started", "cron.delivery.saved"]


def test_cron_run_delivery_workflow_separates_agent_and_delivery_conversation_ids() -> None:
    agent_conversation_ids: list[str] = []
    saved_conversation_ids: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "web",
        delivery_target=lambda job, channel: "web:operator",
        run_agent_turn=lambda job, conv_id: agent_conversation_ids.append(conv_id) or {"text": "Done"},
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: saved_conversation_ids.append(conv_id) or True,
        send_platform_delivery=lambda job, channel, target, text: False,
    )

    result = run_cron_delivery_workflow(
        Job(delivery_channel="web"),
        label="Scheduled task",
        callbacks=callbacks,
        agent_conversation_id="web:operator:background:manual-cron-1",
    )

    assert result["cron_delivery_status"] == "saved"
    assert agent_conversation_ids == ["web:operator:background:manual-cron-1"]
    assert saved_conversation_ids == ["web:operator"]


def test_cron_run_delivery_workflow_requires_messaging_delivery_success() -> None:
    events: list[tuple[str, dict]] = []
    started: list[str] = []
    cleared: list[str] = []
    artifact = Path("/tmp/nullion-cron-output.txt")

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": "Done", "artifacts": [{"path": str(artifact)}]},
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: (
            assert_channel_without_media(channel, text, str(artifact)) and False
        ),
        start_background_delivery=lambda conv_id, job: started.append(conv_id),
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert result["cron_delivery_failed"] is True
    assert started == ["telegram:42"]
    assert cleared == []
    assert events[-1] == ("cron.delivery.failed", {"reason": "platform delivery failed"})
    assert result["execution_outcome"]["status"] == "failed"
    assert result["execution_outcome"]["delivery_succeeded"] is False


def test_cron_run_delivery_workflow_marks_agent_exception_failed() -> None:
    events: list[tuple[str, dict]] = []
    started: list[str] = []
    cleared: list[str] = []

    def fail_agent(job, conv_id):  # noqa: ANN001
        raise TimeoutError("model read timed out")

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=fail_agent,
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: False,
        start_background_delivery=lambda conv_id, job: started.append(conv_id),
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(id="cron-1", delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert result["cron_run_failed"] is True
    assert result["cron_delivery_failed"] is True
    assert result["reason"] == "cron_run_model_timeout"
    assert result["error"] == "model read timed out"
    assert started == ["telegram:42"]
    assert cleared == ["telegram:42"]
    assert events[-1] == ("cron.delivery.failed", {"reason": "cron_run_model_timeout"})
    assert result["execution_outcome"]["status"] == "failed"
    assert result["execution_outcome"]["failure_reason"] == "cron_run_model_timeout"


def test_cron_run_delivery_workflow_preserves_explicit_media_delivery(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact = tmp_path / "nullion-cron-report.json"
    artifact.write_text("{}", encoding="utf-8")

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": f"Done\nMEDIA:{artifact}"},
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [f"⏰ **SCHEDULED TASK: Morning brief**\n\nDone\nMEDIA:{artifact}"]
    assert result["execution_outcome"]["status"] == "succeeded"
    assert result["execution_outcome"]["artifacts_delivered"] == [str(artifact)]


def test_cron_run_delivery_workflow_marks_attachment_failure_after_text_fallback_as_partial_success(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, dict]] = []
    sent: list[str] = []
    cleared: list[str] = []
    artifact = tmp_path / "cron-report.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    def send_platform_delivery(job: Job, channel: str, text: str) -> bool:
        sent.append(text)
        return "MEDIA:" not in text

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": f"Done\nMEDIA:{artifact}"},
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=send_platform_delivery,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "partial_success"
    assert result["cron_delivery_partial_success"] is True
    assert result["cron_delivery_attachment_failed"] is True
    assert "cron_delivery_failed" not in result
    assert sent[0] == f"⏰ **SCHEDULED TASK: Morning brief**\n\nDone\nMEDIA:{artifact}"
    assert len(sent) == 2
    assert "could not attach the report file" in sent[1]
    assert "duplicate long messages" in sent[1]
    assert f"MEDIA:{artifact}" not in sent[1]
    assert cleared == ["telegram:42"]
    assert events[-1] == (
        "cron.delivery.partial_success",
        {"reason": "attachment delivery failed after text fallback"},
    )
    assert result["execution_outcome"]["status"] == "partially_succeeded"
    assert result["execution_outcome"]["failure_reason"] == "attachment delivery failed"


def test_cron_attachment_failure_fallback_does_not_resend_full_report(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact = tmp_path / "cron-report.html"
    artifact.write_text("<html>report</html>", encoding="utf-8")
    long_report = "\n".join(f"Metric line {index}: {'x' * 80}" for index in range(60))

    def send_platform_delivery(job: Job, channel: str, text: str) -> bool:
        sent.append(text)
        return "MEDIA:" not in text

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": f"{long_report}\nMEDIA:{artifact}"},
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=send_platform_delivery,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "partial_success"
    assert len(sent) == 2
    assert len(sent[1]) < 700
    assert "Metric line 59" not in sent[1]
    assert f"MEDIA:{artifact}" not in sent[1]


def test_cron_run_delivery_workflow_attaches_structured_tool_artifact(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact = tmp_path / "latest-news.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    result_payload = {
        "final_text": "Created the latest news summary PDF.",
        "tool_results": [
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {
                    "artifact_path": str(artifact),
                    "artifact_paths": [str(artifact)],
                },
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == (
        f"Created the latest news summary PDF.\n\nMEDIA:{artifact}"
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [f"⏰ **SCHEDULED TASK: Morning brief**\n\nCreated the latest news summary PDF.\n\nMEDIA:{artifact}"]


def test_cron_run_delivery_workflow_attaches_completed_file_write_html_artifact(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "weekly-trends.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<!doctype html><title>Weekly trends</title>", encoding="utf-8")

    result_payload = {
        "final_text": "Created the weekly trend report.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {
                    "path": str(artifact),
                    "bytes_written": artifact.stat().st_size,
                },
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == (
        f"Created the weekly trend report.\n\nMEDIA:{artifact}"
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [f"⏰ **SCHEDULED TASK: Morning brief**\n\nCreated the weekly trend report.\n\nMEDIA:{artifact}"]


def test_cron_delivery_inlines_local_html_images_instead_of_attaching_support_files(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    report = artifact_dir / "provincetown-monitor.html"
    image = artifact_dir / "airbnb_cozy_commercial.png"
    artifact_dir.mkdir(parents=True)
    image.write_bytes(b"\x89PNG\r\n\x1a\nlisting-image")
    report.write_text(
        '<!doctype html><html><body><h1>Report</h1><img src="airbnb_cozy_commercial.png"></body></html>',
        encoding="utf-8",
    )
    result_payload = {
        "final_text": f"Report ready.\n\nMEDIA:{image}",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
            {
                "tool_name": "terminal_exec",
                "status": "completed",
                "output": {"artifact_paths": [str(image)]},
            },
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert delivered == f"Report ready.\n\nMEDIA:{report}"
    html = report.read_text(encoding="utf-8")
    assert 'src="airbnb_cozy_commercial.png"' not in html
    assert 'src="data:image/png;base64,' in html


def test_cron_delivery_suppresses_text_marker_from_linked_asset_folder(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    report = artifact_dir / "random_website_report_linked.html"
    workbook = artifact_dir / "random_website_workbook.xlsx"
    asset_dir = artifact_dir / "random_website_report_linked_assets"
    marker = asset_dir / "placeholder.txt"
    artifact_dir.mkdir(parents=True)
    asset_dir.mkdir()
    report.write_text(
        "<!doctype html><html><body><h1>Linked report</h1></body></html>",
        encoding="utf-8",
    )
    workbook.write_bytes(b"PK\x03\x04workbook")
    marker.write_text("assets folder for linked HTML", encoding="utf-8")
    result_payload = {
        "final_text": "Generated artifacts.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(marker), "bytes_written": marker.stat().st_size},
            },
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
            {
                "tool_name": "spreadsheet_create",
                "status": "completed",
                "output": {"artifact_path": str(workbook)},
            },
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)
    artifact_paths = cron_delivery_artifact_paths_from_result(result_payload, delivered)

    assert str(marker) not in delivered
    assert marker.name not in delivered
    assert str(marker) not in artifact_paths
    assert str(report) in delivered
    assert str(workbook) in delivered


def test_cron_delivery_prefers_explicit_text_manifest_over_intermediate_tool_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_dir.mkdir(parents=True)
    workbook = artifact_dir / "random_website_stress_workbook.xlsx"
    report = artifact_dir / "random_website_stress_report.pdf"
    linked = artifact_dir / "random_website_stress_linked.html"
    derived = artifact_dir / "derived_site_summary.png"
    intermediate = artifact_dir / "generated_derived_image.pdf"
    for path in (workbook, report, linked, derived, intermediate):
        path.write_bytes(b"artifact")

    result_payload = {
        "final_text": (
            "Generated artifacts:\n"
            f"MEDIA:{workbook}\n"
            f"MEDIA:{report}\n"
            f"MEDIA:{linked}\n"
            f"MEDIA:{derived}"
        ),
        "tool_results": [
            {
                "tool_name": "spreadsheet_create",
                "status": "completed",
                "output": {"artifact_path": str(workbook)},
            },
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {"artifact_path": str(report)},
            },
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(linked), "bytes_written": linked.stat().st_size},
            },
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {"artifact_path": str(intermediate)},
            },
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:100001")
    artifact_paths = cron_delivery_artifact_paths_from_result(
        result_payload,
        delivered,
        principal_id="telegram:100001",
    )

    assert str(intermediate) not in delivered
    assert str(intermediate) not in artifact_paths
    assert artifact_paths == (str(workbook), str(report), str(linked), str(derived))


def test_cron_delivery_keeps_full_structured_artifact_set_when_text_mentions_subset(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_dir.mkdir(parents=True)
    workbook = artifact_dir / "random_site_stress_workbook.xlsx"
    document = artifact_dir / "random_site_stress_report.docx"
    deck = artifact_dir / "random_site_stress_deck.pptx"
    report = artifact_dir / "random_site_stress_report.pdf"
    html_report = artifact_dir / "random_site_stress_report.html"
    csv_rows = artifact_dir / "random_site_stress_rows.csv"
    for path in (workbook, document, deck, report, html_report, csv_rows):
        path.write_bytes(b"artifact")

    result_payload = {
        "final_text": f"Generated and verified the media report.\n\nMEDIA:{report}",
        "tool_results": [
            {
                "tool_name": "spreadsheet_create",
                "status": "completed",
                "output": {"artifact_path": str(workbook)},
            },
            {
                "tool_name": "document_create",
                "status": "completed",
                "output": {"artifact_path": str(document)},
            },
            {
                "tool_name": "presentation_create",
                "status": "completed",
                "output": {"artifact_path": str(deck)},
            },
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {"artifact_path": str(report)},
            },
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(html_report), "bytes_written": html_report.stat().st_size},
            },
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(csv_rows), "bytes_written": csv_rows.stat().st_size},
            },
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:100001")
    artifact_paths = cron_delivery_artifact_paths_from_result(
        result_payload,
        delivered,
        principal_id="telegram:100001",
    )

    assert artifact_paths == (
        str(report),
        str(workbook),
        str(document),
        str(deck),
        str(html_report),
        str(csv_rows),
    )
    for path in artifact_paths:
        assert f"MEDIA:{path}" in delivered


def test_cron_run_delivery_workflow_sends_single_self_contained_html_artifact(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    report = artifact_dir / "provincetown-monitor.html"
    image = artifact_dir / "airbnb_frenchie.png"
    artifact_dir.mkdir(parents=True)
    image.write_bytes(b"\x89PNG\r\n\x1a\nlisting-image")
    report.write_text(
        "<!doctype html><html><body><img src='airbnb_frenchie.png'><p>Ready</p></body></html>",
        encoding="utf-8",
    )
    result_payload = {
        "final_text": "Created the Provincetown report.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
            {
                "tool_name": "terminal_exec",
                "status": "completed",
                "output": {"artifact_paths": [str(image)]},
            },
        ],
    }
    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [f"⏰ **SCHEDULED TASK: Morning brief**\n\nCreated the Provincetown report.\n\nMEDIA:{report}"]
    assert "data:image/png;base64," in report.read_text(encoding="utf-8")


def test_cron_delivery_auto_mode_inlines_remote_html_images_from_collected_source(monkeypatch, tmp_path: Path) -> None:
    directory = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    directory.mkdir(parents=True)
    report = directory / "fixture.html"
    image = directory / "collected.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\ncollected-image")
    url = "https://images.example.test/fixture.png"
    report.write_text(f"<!doctype html><html><body><img src='{url}'><p>Fixture</p></body></html>")
    payload = {"final_text":"Created the report.", "tool_results":[
        {"tool_name":"file_download","status":"completed","output":{"url":url,"path":str(image)}},
        {"tool_name":"file_write","status":"completed","output":{"path":str(report),"bytes_written":report.stat().st_size}}]}
    def forbidden(*args, **kwargs):
        raise AssertionError("Delivery must reuse collected bytes, without new network requests")
    monkeypatch.setattr("nullion.cron_delivery.urlopen",forbidden)
    text=cron_delivery_text_from_result(payload,html_image_delivery_mode="auto")
    assert f"MEDIA:{report}" in text
    assert url not in report.read_text()
    assert "data:image/png;base64," in report.read_text()


def test_cron_delivery_keeps_remote_html_images_when_linked_mode_requested(monkeypatch, tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    report = artifact_dir / "provincetown-monitor-linked.html"
    artifact_dir.mkdir(parents=True)
    report.write_text(
        "<!doctype html><html><body><img src='https://cdn.example.com/listing.png'></body></html>",
        encoding="utf-8",
    )

    def _fail_if_called(*_args, **_kwargs):
        raise AssertionError("linked mode should not fetch remote images")

    monkeypatch.setattr("nullion.cron_delivery.urlopen", _fail_if_called)
    result_payload = {
        "final_text": "Created the Provincetown report.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
        ],
    }

    assert cron_delivery_text_from_result(
        result_payload,
        html_image_delivery_mode="linked",
    ) == f"Created the Provincetown report.\n\nMEDIA:{report}"
    html = report.read_text(encoding="utf-8")
    assert "https://cdn.example.com/listing.png" in html
    assert "data:image/png;base64," not in html


def test_cron_delivery_inlines_remote_html_images_when_self_contained_requested(monkeypatch, tmp_path: Path) -> None:
    directory = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    directory.mkdir(parents=True)
    report = directory / "fixture.html"
    image = directory / "collected.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\ncollected-image")
    url = "https://images.example.test/fixture.png"
    report.write_text(f"<!doctype html><html><body><img src='{url}'><p>Fixture</p></body></html>")
    payload = {"final_text":"Created the report.", "tool_results":[
        {"tool_name":"file_download","status":"completed","output":{"url":url,"path":str(image)}},
        {"tool_name":"file_write","status":"completed","output":{"path":str(report),"bytes_written":report.stat().st_size}}]}
    def forbidden(*args, **kwargs):
        raise AssertionError("Delivery must reuse collected bytes, without new network requests")
    monkeypatch.setattr("nullion.cron_delivery.urlopen",forbidden)
    text=cron_delivery_text_from_result(payload,html_image_delivery_mode="self_contained")
    assert f"MEDIA:{report}" in text
    assert url not in report.read_text()
    assert "data:image/png;base64," in report.read_text()


def test_cron_delivery_uses_job_html_image_mode_when_result_has_no_override(monkeypatch, tmp_path: Path) -> None:
    directory = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    directory.mkdir(parents=True)
    report = directory / "fixture.html"
    image = directory / "collected.png"
    image.write_bytes(b"\x89PNG\r\n\x1a\ncollected-image")
    url = "https://images.example.test/fixture.png"
    report.write_text(f"<!doctype html><html><body><img src='{url}'><p>Fixture</p></body></html>")
    payload = {"final_text":"Created the report.", "tool_results":[
        {"tool_name":"file_download","status":"completed","output":{"url":url,"path":str(image)}},
        {"tool_name":"file_write","status":"completed","output":{"path":str(report),"bytes_written":report.stat().st_size}}]}
    def forbidden(*args, **kwargs):
        raise AssertionError("Delivery must reuse collected bytes, without new network requests")
    monkeypatch.setattr("nullion.cron_delivery.urlopen",forbidden)
    callbacks = CronRunDeliveryCallbacks(effective_channel=lambda job:"telegram", delivery_target=lambda job,channel:"42",
        run_agent_turn=lambda job,conv_id:payload,record_event=lambda *a,**kw:None,
        block_reason=lambda *a:None,save_web_delivery=lambda *a:False,send_platform_delivery=lambda *a:True)
    result=run_cron_delivery_workflow(Job(delivery_channel="telegram",html_image_delivery_mode="self_contained"),label="Scheduled task",callbacks=callbacks)
    assert result['cron_delivery_status']=='sent'
    assert url not in report.read_text()
    assert "data:image/png;base64," in report.read_text()


def test_cron_delivery_replaces_unreachable_remote_html_images_with_inline_placeholder_when_opted_in(
    monkeypatch,
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    report = artifact_dir / "provincetown-monitor-remote-fallback.html"
    artifact_dir.mkdir(parents=True)
    report.write_text(
        "<!doctype html><html><body><img src='https://cdn.example.com/missing.jpg'></body></html>",
        encoding="utf-8",
    )
    def _raise_urlopen(*_args, **_kwargs):
        raise OSError("boom")

    monkeypatch.setattr("nullion.cron_delivery.urlopen", _raise_urlopen)

    result_payload = {
        "final_text": "Created the Provincetown report.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
        ],
    }

    assert cron_delivery_text_from_result(
        result_payload,
        html_image_delivery_mode="self_contained",
    ) == f"Created the Provincetown report.\n\nMEDIA:{report}"
    html = report.read_text(encoding="utf-8")
    assert "https://cdn.example.com/missing.jpg" not in html
    assert "data:image/svg+xml;base64," in html


def test_cron_artifact_validation_self_contained_mode_rewrites_remote_images(tmp_path: Path) -> None:
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "remote-image-report.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<!doctype html><html><body><img src='https://example.com/remote.png'></body></html>", encoding="utf-8")
    result_payload = {
        "final_text": "Report ready.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(artifact), "bytes_written": artifact.stat().st_size},
            },
        ],
    }

    text = cron_delivery_text_from_result(
        result_payload,
        principal_id="telegram:100001",
        html_image_delivery_mode="self_contained",
    )
    reason = cron_artifact_validation_block_reason(
        result_payload,
        text,
        principal_id="telegram:100001",
        html_image_delivery_mode="self_contained",
    )

    assert reason is None
    assert result_payload.get("cron_artifact_validation_errors") in (None, [])
    html = artifact.read_text(encoding="utf-8")
    assert "https://example.com/remote.png" not in html
    assert "data:image/" in html


def test_cron_artifact_validation_adds_idempotent_mobile_overflow_guard(tmp_path: Path) -> None:
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "wide-report.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(
        "<!doctype html><html><head><title>Wide</title></head><body>"
        "<table><tr><td>https://example.test/a-very-long-unbroken-value</td></tr></table>"
        "</body></html>",
        encoding="utf-8",
    )
    result_payload = {
        "final_text": "Report ready.",
        "tool_results": [
            ToolResult("write-1", "file_write", "completed", {"path": str(artifact)}),
        ],
    }
    text = cron_delivery_text_from_result(result_payload)

    assert cron_artifact_validation_block_reason(result_payload, text) is None
    assert cron_artifact_validation_block_reason(result_payload, text) is None
    html = artifact.read_text(encoding="utf-8")
    assert html.count('id="nullion-responsive-delivery-guard-v2"') == 1
    assert "overflow-x:auto!important" in html
    assert "table-layout:auto!important" in html
    assert "overflow-wrap:anywhere!important" in html
    assert result_payload.get("cron_responsive_html_guard_applied") in (None, [])


def test_cron_delivery_does_not_auto_attach_browser_verification_screenshot(tmp_path: Path) -> None:
    report = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "premarket_stock_report.html"
    screenshot = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "screenshot-verification.png"
    report.parent.mkdir(parents=True)
    report.write_text("<!doctype html><html><body>Pre-market report</body></html>", encoding="utf-8")
    screenshot.write_bytes(b"\x89PNG\r\nverification")

    result_payload = {
        "final_text": "Pre-market stock report generated and verified readable.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            },
            {
                "tool_name": "browser_screenshot",
                "status": "completed",
                "output": {
                    "path": str(screenshot),
                    "artifact_path": str(screenshot),
                    "artifact_paths": [str(screenshot)],
                    "format": "png",
                },
            },
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == (
        f"Pre-market stock report generated and verified readable.\n\nMEDIA:{report}"
    )


def test_cron_delivery_blocks_script_rendered_primary_html_content(tmp_path: Path) -> None:
    report = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "dynamic_report.html"
    report.parent.mkdir(parents=True)
    report.write_text(
        (
            "<!doctype html><html><body>"
            "<h1>Report</h1><div id='cards'></div>"
            "<script>const rows=[{title:'Only in script'}];"
            "document.getElementById('cards').innerHTML=rows.map(r=>`<article>${r.title}</article>`).join('');"
            "</script></body></html>"
        ),
        encoding="utf-8",
    )
    result_payload = {
        "final_text": "Report generated.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            }
        ],
    }

    text = cron_delivery_text_from_result(result_payload)
    reason = cron_artifact_validation_block_reason(result_payload, text)

    assert reason == "cron_artifact_validation_failed"
    assert result_payload["cron_artifact_validation_errors"][0]["code"] == "html_primary_content_script_dependent"


def test_cron_delivery_blocks_html_missing_typed_required_content_tokens(tmp_path: Path) -> None:
    report = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "partial-report.html"
    report.parent.mkdir(parents=True)
    report.write_text(
        '<!doctype html><html><head><meta name="NVDA"></head><body><article>MSFT</article></body></html>',
        encoding="utf-8",
    )
    result_payload = {
        "final_text": "Report generated.",
        "required_artifact_content_tokens": ["MSFT", "NVDA"],
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            }
        ],
    }

    text = cron_delivery_text_from_result(result_payload)
    reason = cron_artifact_validation_block_reason(result_payload, text)

    assert reason == "cron_artifact_validation_failed"
    assert result_payload["cron_artifact_validation_errors"][-1]["code"] == "artifact_required_content_missing"
    assert "NVDA" in result_payload["cron_artifact_validation_errors"][-1]["message"]


def test_cron_artifact_validation_ignores_scratch_files_excluded_by_typed_extension_contract(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_root.mkdir(parents=True)
    report = artifact_root / "social-report.html"
    report.write_text(
        "<!doctype html><html><head><title>Report</title></head>"
        "<body><main><h1>GitHub Traffic Trend</h1><section>Reddit</section></main></body></html>",
        encoding="utf-8",
    )
    scratch = artifact_root / "terminal-output.txt"
    scratch.write_text("internal working output", encoding="utf-8")
    result_payload = {
        "final_text": "Report ready.",
        "required_artifact_content_tokens": ["GitHub Traffic Trend", "Reddit"],
        "required_artifact_extensions": [".html"],
        "deliver_required_extensions_only": True,
        "tool_results": [
            ToolResult("write-1", "file_write", "completed", {"path": str(report)}),
            ToolResult("write-2", "file_write", "completed", {"path": str(scratch)}),
        ],
    }
    job = Job(
        artifact_delivery_options={
            "requires_attachment_delivery": True,
            "required_artifact_extensions": [".html"],
            "deliver_required_extensions_only": True,
        }
    )

    text = _filter_cron_delivery_media_to_required_extensions(
        job,
        cron_delivery_text_from_result(result_payload),
    )

    assert text == f"Report ready.\n\nMEDIA:{report}"
    assert cron_artifact_validation_block_reason(result_payload, text) is None
    assert result_payload.get("cron_artifact_validation_errors") in (None, [])


def test_cron_delivery_blocks_csv_with_inconsistent_columns(tmp_path: Path) -> None:
    report = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "bad_urls.csv"
    report.parent.mkdir(parents=True)
    report.write_text(
        "site,title,url\n"
        "NASA,Challenge,https://www.nasa.gov/wp-content/uploads/image.jpg?resize=2048,1365\n",
        encoding="utf-8",
    )
    result_payload = {
        "final_text": f"CSV generated.\n\nMEDIA:{report}",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(report), "bytes_written": report.stat().st_size},
            }
        ],
    }

    text = cron_delivery_text_from_result(result_payload)
    reason = cron_artifact_validation_block_reason(result_payload, text)

    assert reason == "cron_artifact_validation_failed"
    assert result_payload["cron_artifact_validation_errors"][0]["code"] == "csv_inconsistent_columns"


def test_cron_agent_prompt_requires_valid_csv_and_distinct_html_delivery_contracts() -> None:
    prompt = cron_agent_prompt(
        Job(
            name="Artifact stress run",
            task="Create CSV, self-contained HTML, linked HTML, and reports.",
        ),
        label="Scheduled task",
    )

    assert "CSV rows must parse with the same column count on every row" in prompt
    assert "the linked/non-self-contained file must reference local sibling asset files by relative path" in prompt
    assert "Do not label an HTML artifact as linked when its images are all data URIs" in prompt
    assert "Do not say images or support files are attached separately unless" in prompt


def test_cron_delivery_preserves_explicit_browser_screenshot_media(tmp_path: Path) -> None:
    screenshot = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "requested-screenshot.png"
    screenshot.parent.mkdir(parents=True)
    screenshot.write_bytes(b"\x89PNG\r\nrequested")

    result_payload = {
        "final_text": f"Screenshot captured.\n\nMEDIA:{screenshot}",
        "tool_results": [
            {
                "tool_name": "browser_screenshot",
                "status": "completed",
                "output": {
                    "path": str(screenshot),
                    "artifact_path": str(screenshot),
                    "artifact_paths": [str(screenshot)],
                },
            },
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == f"Screenshot captured.\n\nMEDIA:{screenshot}"


def test_cron_run_delivery_workflow_does_not_attach_file_read_html_artifact(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "public-awareness.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<!doctype html><html><body>Nullion audience report</body></html>", encoding="utf-8")

    result_payload = {
        "final_text": "Verified the HTML report.",
        "tool_results": [
            {
                "tool_name": "file_read",
                "status": "completed",
                "output": {
                    "path": str(artifact),
                    "content": artifact.read_text(encoding="utf-8"),
                },
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == "Verified the HTML report."

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == ["⏰ **SCHEDULED TASK: Morning brief**\n\nVerified the HTML report."]


def test_cron_run_delivery_workflow_only_attaches_written_report_after_reading_previous_report(tmp_path: Path) -> None:
    sent: list[str] = []
    artifact_root = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    previous_report = artifact_root / "nullion_public_awareness_github_audience_2026-05-06_1600.html"
    current_report = artifact_root / "nullion_public_awareness_github_audience_2026-05-06_1700.html"
    artifact_root.mkdir(parents=True)
    previous_report.write_text("<!doctype html><html><body>4 PM report</body></html>", encoding="utf-8")
    current_report.write_text("<!doctype html><html><body>5 PM report</body></html>", encoding="utf-8")

    result_payload = {
        "final_text": "Nullion monitor ran successfully.",
        "tool_results": [
            {
                "tool_name": "file_read",
                "status": "completed",
                "output": {
                    "path": str(previous_report),
                    "content": previous_report.read_text(encoding="utf-8"),
                },
            },
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(current_report), "bytes_written": current_report.stat().st_size},
            },
            {
                "tool_name": "file_read",
                "status": "completed",
                "output": {
                    "path": str(current_report),
                    "content": current_report.read_text(encoding="utf-8"),
                },
            },
        ],
    }

    delivered_text = cron_delivery_text_from_result(result_payload)

    assert str(previous_report) not in delivered_text
    assert previous_report.name not in delivered_text
    assert delivered_text == (
        "Nullion monitor ran successfully.\n\n"
        f"MEDIA:{current_report}"
    )

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [scheduled_task_delivery_text(Job(), delivered_text)]


def test_cron_run_delivery_workflow_does_not_attach_file_write_workspace_file(tmp_path: Path) -> None:
    state_file = tmp_path / "workspaces" / "workspace_admin" / "files" / "weekly-trends.json"
    state_file.parent.mkdir(parents=True)
    state_file.write_text("{}", encoding="utf-8")

    result_payload = {
        "final_text": "Updated weekly state.",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {
                    "path": str(state_file),
                    "bytes_written": state_file.stat().st_size,
                },
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == "Updated weekly state."


def test_cron_run_delivery_workflow_normalizes_split_artifact_directive(tmp_path: Path) -> None:
    artifact = tmp_path / "latest-news.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    result_payload = {
        "final_text": f"Created the latest news summary PDF.\n\nMEDIA\n{artifact}",
        "tool_results": [
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {"artifact_path": str(artifact)},
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == (
        f"Created the latest news summary PDF.\n\nMEDIA:{artifact}"
    )


def test_cron_run_delivery_workflow_normalizes_same_line_artifact_directive(tmp_path: Path) -> None:
    artifact = tmp_path / "latest-news.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    result_payload = {
        "final_text": f"生成しました\nMEDIA {artifact}",
        "tool_results": [
            {
                "tool_name": "pdf_create",
                "status": "completed",
                "output": {"artifact_path": str(artifact)},
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == f"生成しました\nMEDIA:{artifact}"


def test_platform_delivery_removes_split_media_block_from_attachment_caption(tmp_path: Path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion-artifact-latest-news.pdf"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")
    reply = (
        "⏰ Scheduled task: Hourly CNN latest news PDF\n\n"
        "CNN latest news summary PDF created successfully.\n\n"
        f"MEDIA\n{artifact}\n\n"
        f"MEDIA:{artifact}"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.text == (
        "⏰ **SCHEDULED TASK: Hourly CNN latest news PDF**\n\n"
        "CNN latest news summary PDF created successfully."
    )
    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_removes_same_line_media_block_from_attachment_caption(tmp_path: Path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion-artifact-latest-news.pdf"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")
    reply = f"⏰ Scheduled task: Hourly CNN latest news PDF\n\n生成しました\n\nMEDIA {artifact}"

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.text == "⏰ **SCHEDULED TASK: Hourly CNN latest news PDF**\n\n生成しました"
    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0


def test_platform_delivery_removes_relative_artifact_media_from_cron_caption(tmp_path: Path, monkeypatch) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "cnn_latest_news_2026-05-04_0950.pdf"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    reply = (
        "⏰ Scheduled task: CNN latest news PDF every 5 minutes\n\n"
        "CNN latest news PDF generated.\n\n"
        "MEDIA artifacts/cnn_latest_news_2026-05-04_0950.pdf"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.text == (
        "⏰ **SCHEDULED TASK: CNN latest news PDF every 5 minutes**\n\n"
        "CNN latest news PDF generated."
    )
    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0
    assert "MEDIA" not in (delivery.text or "")
    assert "artifacts/cnn_latest_news_2026-05-04_0950.pdf" not in (delivery.text or "")


def test_platform_delivery_resolves_bare_media_filename_from_workspace_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from nullion.messaging_adapters import prepare_reply_for_platform_delivery

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion_public_awareness_github_traffic_monitor.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")

    reply = (
        "⏰ Manual scheduled task run: Nullion social + GitHub traffic monitor\n\n"
        "Run complete.\n\n"
        "MEDIA nullion_public_awareness_github_traffic_monitor.html"
    )

    delivery = prepare_reply_for_platform_delivery(reply, principal_id="telegram:100001")

    assert delivery.attachments == (artifact,)
    assert delivery.media_directive_count == 1
    assert delivery.unavailable_attachment_count == 0
    assert "MEDIA" not in (delivery.text or "")
    assert "nullion_public_awareness_github_traffic_monitor.html" not in (delivery.text or "")


def test_cron_delivery_resolves_bare_media_filename_before_marking_sent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion_public_awareness_github_traffic_monitor.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")
    sent: list[str] = []
    events: list[tuple[str, dict]] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "100001",
        run_agent_turn=lambda job, conv_id: {
            "final_text": "Run complete.\nMEDIA nullion_public_awareness_github_traffic_monitor.html",
            "tool_results": [ToolResult("render", "terminal_exec", "completed", {})],
        },
        record_event=lambda event_type, job, channel, target, conv_id, **extra: events.append((event_type, extra)),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Manual scheduled task run", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [f"⏰ **MANUAL SCHEDULED TASK RUN: Morning brief**\n\nRun complete.\nMEDIA:{artifact}"]
    assert events[-1][0] == "cron.delivery.sent"


def test_cron_delivery_promotes_plain_report_artifact_path_before_marking_sent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    artifact = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "nullion_social_github_traffic_monitor.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("<html><body>" + ("traffic " * 20) + "</body></html>", encoding="utf-8")
    sent: list[str] = []
    events: list[tuple[str, dict]] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "100001",
        run_agent_turn=lambda job, conv_id: {
            "final_text": (
                "Nullion monitor generated\n"
                "GitHub via gh: stars 13, forks 3.\n"
                "Report: artifacts/nullion_social_github_traffic_monitor.html"
            ),
            "tool_results": [ToolResult("render", "terminal_exec", "completed", {})],
        },
        record_event=lambda event_type, job, channel, target, conv_id, **extra: events.append((event_type, extra)),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Manual scheduled task run", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert sent == [
        "⏰ **MANUAL SCHEDULED TASK RUN: Morning brief**\n\n"
        "Nullion monitor generated\n"
        "GitHub via gh: stars 13, forks 3.\n\n"
        f"MEDIA:{artifact}"
    ]
    assert "artifacts/nullion_social_github_traffic_monitor.html" not in sent[0].split("\n\nMEDIA:", 1)[0]
    assert cron_delivery_artifact_paths_from_result(
        callbacks.run_agent_turn(Job(), "telegram:100001"),
        sent[0],
        principal_id="telegram:100001",
    ) == (str(artifact),)
    assert events[-1][0] == "cron.delivery.sent"


def test_cron_delivery_fails_unresolved_bare_media_filename_without_false_sent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path))
    sent: list[str] = []
    events: list[tuple[str, dict]] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "100001",
        run_agent_turn=lambda job, conv_id: {
            "final_text": "Run complete.\nMEDIA missing_report.html",
            "tool_results": [ToolResult("render", "terminal_exec", "completed", {})],
        },
        record_event=lambda event_type, job, channel, target, conv_id, **extra: events.append((event_type, extra)),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Manual scheduled task run", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert result["reason"] == "cron_artifact_validation_failed"
    assert sent == []
    assert events[-1] == ("cron.delivery.failed", {"reason": "cron_artifact_validation_failed"})


def test_cron_delivery_normalizes_orphan_media_marker_with_structured_artifact(tmp_path: Path) -> None:
    from nullion.tools import ToolResult

    artifact = tmp_path / "cnn_screenshot.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")
    result_payload = {
        "final_text": "CNN screenshot PDF created successfully.\nMEDIA",
        "tool_results": [
            ToolResult("pdf-create", "pdf_create", "completed", {"artifact_path": str(artifact)})
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == (
        f"CNN screenshot PDF created successfully.\n\nMEDIA:{artifact}"
    )


def test_cron_run_delivery_workflow_suppresses_state_file_only_media_delivery() -> None:
    sent: list[str] = []
    cleared: list[str] = []
    state_path = "/workspace/files/payment_reminder_tracker.json"
    artifact_path = "/workspace/artifacts/payment_reminder_tracker.json"

    result_payload = {
        "final_text": f"saved state\n\nMEDIA:{state_path}\nMEDIA:{artifact_path}",
        "artifacts": [{"path": artifact_path}],
        "tool_results": [
            {"tool_name": "file_read", "output": {"path": state_path}},
            {"tool_name": "file_write", "output": {"path": artifact_path}},
        ],
    }
    assert cron_delivery_text_from_result(result_payload) == ""

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "silent"
    assert sent == []
    assert cleared == ["telegram:42"]


def test_scheduled_cron_empty_text_delivers_structured_state_summary(tmp_path: Path) -> None:
    events: list[str] = []
    sent: list[str] = []
    cleared: list[str] = []
    state_dir = tmp_path / "workspace" / "files"
    state_dir.mkdir(parents=True)
    state_path = state_dir / "payment_due_reminder_state.json"
    state_path.write_text(
        json.dumps(
            {
                "run_date": "2026-06-09",
                "action": "duplicate_suppressed",
                "overdue_unconfirmed_items": [{"key": "one"}],
                "confirmed_items_seen": [{}, {}],
                "not_in_due_window": [{}],
            }
        ),
        encoding="utf-8",
    )
    result_payload = {
        "final_text": "",
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(state_path)},
            },
        ],
    }

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: result_payload,
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(
        Job(delivery_channel="telegram", name="Payment due reminder check"),
        label="Scheduled task",
        callbacks=callbacks,
    )

    assert result["cron_delivery_status"] == "sent"
    assert result["cron_silent_state_result_replaced"] is True
    assert "cron.delivery.silent" not in events
    assert "cron.delivery.sent" in events
    assert cleared == ["telegram:42"]
    assert len(sent) == 1
    assert "**SCHEDULED TASK: Payment due reminder check**" in sent[0]
    assert "structured run state" in sent[0]
    assert "Action: duplicate suppressed" in sent[0]
    assert "overdue unconfirmed items: 1" in sent[0]
    assert "confirmed items seen: 2" in sent[0]
    assert str(state_path) not in sent[0]
    assert "payment_due_reminder_state.json" not in sent[0]


def test_cron_run_delivery_workflow_suppresses_workspace_files_state_media() -> None:
    state_path = "/Users/example/.nullion/workspaces/workspace_admin/files/jimmy_eat_world_ticket_monitor_state.json"
    result_payload = {
        "final_text": (
            "Jimmy Eat World ticket deal update:\n"
            "• Vivid Seats shows a lower price.\n\n"
            f"MEDIA:{state_path}"
        ),
        "tool_results": [
            {"tool_name": "file_write", "status": "completed", "output": {"path": state_path}},
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:100001")

    assert "Jimmy Eat World ticket deal update" in delivered
    assert "MEDIA:" not in delivered
    assert "monitor_state.json" not in delivered
    assert cron_delivery_artifact_paths_from_result(
        result_payload,
        delivered,
        principal_id="telegram:100001",
    ) == ()


def test_cron_delivery_does_not_attach_workspace_artifacts_json_state(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "workspaces" / "workspace_admin" / "artifacts"
    artifact_dir.mkdir(parents=True)
    state_path = artifact_dir / "payment_due_reminder_state_2026-06-12.json"
    state_path.write_text('{"status":"checked"}', encoding="utf-8")
    result_payload = {
        "final_text": (
            "📅 💸 Payment due reminder\n"
            "Overdue/unconfirmed: KM Properties / OLRP rent\n"
            "Amount: $3,600.00"
        ),
        "tool_results": [
            {
                "tool_name": "file_write",
                "status": "completed",
                "output": {"path": str(state_path), "bytes_written": state_path.stat().st_size},
            },
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:100001")

    assert "Payment due reminder" in delivered
    assert "MEDIA:" not in delivered
    assert state_path.name not in delivered
    assert cron_delivery_artifact_paths_from_result(
        result_payload,
        delivered,
        principal_id="telegram:100001",
    ) == ()


def test_cron_delivery_blocks_denied_internal_scheduler_capability() -> None:
    result_payload = {
        "text": "fallback text",
        "tool_results": [
            {
                "tool_name": "set_reminder",
                "status": "denied",
                "output": {
                    "reason": "cron_execution_capability_denied",
                    "denied_capability_tags": ["scheduler"],
                    "tool_capability_tags": ["scheduler", "reminder"],
                },
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None)
        == "cron_run_denied_internal_capability"
    )


def test_cron_delivery_blocks_runs_with_only_failed_tool_evidence() -> None:
    result_payload = {
        "text": "fallback text",
        "tool_results": [
            {
                "tool_name": "connector_request",
                "status": "failed",
                "output": {},
                "error": "provider unavailable",
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None)
        == "cron_run_without_completed_tool_evidence"
    )


def test_cron_delivery_blocks_raw_tool_payload_fallback_from_completed_connector() -> None:
    result_payload = {
        "text": "I checked connector data and received 1 structured item.",
        "raw_tool_payload_blocked": True,
        "tool_results": [
            {
                "tool_name": "connector_request",
                "status": "completed",
                "output": {"messages": [{"id": "msg-1", "subject": "Deadline"}]},
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None)
        == "cron_run_raw_tool_payload"
    )


def test_cron_final_repair_triggers_when_account_identifier_leaks() -> None:
    internal_id = "19e4e04ed4b961f3"
    draft = (
        "Reminder sent 2026-06-16: American Express card ending 15007 due 2026-06-14; "
        "source calendar/Gmail statement Important Notice: Your May 2026 Statement, "
        f"Gmail id {internal_id}. No payment confirmation found in recent Gmail/calendar searches."
    )
    repaired = (
        "Payment due reminder\n\n"
        "Overdue / unconfirmed: American Express card ending 15007\n"
        "Due date: Jun 14, 2026\n"
        "Status: no payment confirmation found in the checked account sources."
    )
    result_payload = {
        "text": draft,
        "tool_results": [
            {
                "tool_name": "email_search",
                "status": "completed",
                "output": {
                    "messages": [
                        {
                            "id": internal_id,
                            "subject": "Important Notice: Your May 2026 Statement",
                            "from": "American Express",
                            "date": "2026-06-14T12:00:00Z",
                            "snippet": "Your May 2026 statement is available.",
                        }
                    ]
                },
            }
        ],
    }
    stages: list[tuple[str, dict[str, object]]] = []

    class RepairModel:
        def create(self, **_kwargs):  # noqa: ANN001
            return {"content": [{"type": "text", "text": json.dumps({"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"], "report": repaired})}]}

    _repair_cron_agent_final_text_if_needed(
        Job(name="Payment due reminder check", task="Check payment due reminders."),
        result_payload,
        model_client=RepairModel(),
        record_preflight=lambda stage, **payload: stages.append((stage, payload)),
    )

    assert result_payload["cron_agent_final_repair_reason"] == "account_identifier_dumped"
    assert result_payload["cron_agent_final_repaired"] is True
    assert result_payload["text"] == repaired
    assert internal_id not in result_payload["text"]
    assert [stage for stage, _payload in stages] == ["final_repair_started", "final_repair_attempt", "final_repaired"]


def test_cron_delivery_blocks_unrepaired_account_identifier_leak() -> None:
    internal_id = "19e4e04ed4b961f3"
    draft = (
        "Reminder sent 2026-06-16: American Express card ending 15007 due 2026-06-14; "
        f"source Gmail id {internal_id}."
    )
    result_payload = {
        "final_text": draft,
        "tool_results": [
            {
                "tool_name": "email_search",
                "status": "completed",
                "output": {"messages": [{"id": internal_id, "subject": "Statement notice"}]},
            }
        ],
    }

    delivered_text = cron_delivery_text_from_result(result_payload)

    assert internal_id not in delivered_text
    assert "account message identifiers" in delivered_text
    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=delivered_text)
        == "cron_run_raw_tool_payload"
    )


def test_cron_delivery_blocks_model_repeated_connector_sanitizer_fallback() -> None:
    connector_output = {
        "messages": [
            {
                "id": "msg-1",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "billing@example.test"},
                        {"name": "Subject", "value": "Account notice"},
                    ]
                },
                "snippet": "Review your account notice.",
            }
        ]
    }
    fallback = safe_raw_tool_payload_replacement(
        tool_results=[ToolResult("connector-1", "connector_request", "completed", connector_output)],
        source="agent",
    )
    result_payload = {
        "text": fallback,
        "tool_results": [
            {
                "tool_name": "connector_request",
                "status": "completed",
                "output": connector_output,
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=fallback)
        == "cron_run_raw_tool_payload"
    )


def test_cron_delivery_blocks_structured_evidence_fallback() -> None:
    tool_result = ToolResult(
        "quote-1",
        "stock_quote",
        "completed",
        {"items": [{"name": "Critical Metals Corp", "price": "$9.41"}]},
    )
    fallback = sanitize_user_visible_reply(
        user_message="Generate the mid-market stock alert.",
        reply="The watchlist is mostly quiet.",
        tool_results=[tool_result],
    )
    result_payload = {"text": fallback, "tool_results": [tool_result]}

    assert fallback == "Results I found\n\n1. Critical Metals Corp\n   $9.41"
    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=fallback)
        == "cron_run_raw_tool_payload"
    )


def test_cron_delivery_blocks_web_search_leads_fallback() -> None:
    cron_task = "Generate a concise mid-market stock alert from verified market data."
    tool_result = ToolResult(
        "search-1",
        "web_search",
        "completed",
        {
            "query": "CRML MSFT OKLO mid market stock price quote news",
            "results": [
                {
                    "title": "Netflix: NFLX Stock Price Quote & News - Robinhood",
                    "url": "https://robinhood.com/stocks/NFLX/",
                    "snippet": "Advertisement Recent share performance and business snapshot Netflix (NFLX) has drawn attention.",
                },
                {
                    "title": "Critical Metals Corp. (CRML) Stock Price, News, Quote & History",
                    "url": "https://finance.yahoo.com/quote/CRML/",
                    "snippet": "Trailing total returns as of 6/15/2026, which may include dividends.",
                },
                {
                    "title": "Oklo: OKLO Stock Price Quote & News - Robinhood",
                    "url": "https://robinhood.com/stocks/OKLO/",
                    "snippet": "As of 2026-06-16, Oklo stock has fluctuated between $58.02 and $61.95.",
                },
            ],
        },
    )
    fallback = sanitize_user_visible_reply(
        user_message=cron_task,
        reply="The market alert is ready.",
        tool_results=[tool_result],
    )
    result_payload = {"text": fallback, "tool_results": [tool_result], "cron_task": cron_task}

    assert fallback is not None
    assert fallback.startswith("Unverified values from structured search snippets")
    assert "Search leads I found" not in fallback
    assert "vehicle type" not in fallback
    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=fallback)
        == "cron_run_raw_tool_payload"
    )


def test_cron_final_repair_triggers_for_web_search_leads_fallback() -> None:
    cron_task = "Generate a concise mid-market stock alert from verified market data."
    tool_result = ToolResult(
        "search-1",
        "web_search",
        "completed",
        {
            "query": "CRML MSFT OKLO mid market stock price quote news",
            "results": [
                {
                    "title": "Netflix: NFLX Stock Price Quote & News - Robinhood",
                    "url": "https://robinhood.com/stocks/NFLX/",
                    "snippet": "Advertisement Recent share performance and business snapshot Netflix (NFLX) has drawn attention.",
                },
                {
                    "title": "Critical Metals Corp. (CRML) Stock Price, News, Quote & History",
                    "url": "https://finance.yahoo.com/quote/CRML/",
                    "snippet": "Trailing total returns as of 6/15/2026, which may include dividends.",
                },
                {
                    "title": "Oklo: OKLO Stock Price Quote & News - Robinhood",
                    "url": "https://robinhood.com/stocks/OKLO/",
                    "snippet": "As of 2026-06-16, Oklo stock has fluctuated between $58.02 and $61.95.",
                },
            ],
        },
    )
    fallback = sanitize_user_visible_reply(
        user_message=cron_task,
        reply="The market alert is ready.",
        tool_results=[tool_result],
    )
    repaired_report = "📈 Mid-market stock check\n\nWatchlist pulse:\n- No verified quote rows were returned."

    class FakeModelClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            return {"content": [{"type": "text", "text": json.dumps({"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"], "report": repaired_report})}]}

    events: list[tuple[str, dict[str, object]]] = []
    result_payload = {"text": fallback, "tool_results": [tool_result]}

    _repair_cron_agent_final_text_if_needed(
        Job(name="Mid-Market Stock Alert", task=cron_task),
        result_payload,
        model_client=FakeModelClient(),
        record_preflight=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
    )

    assert result_payload["cron_agent_final_repaired"] is True
    assert result_payload["cron_agent_final_repair_reason"] == "structured_evidence_fallback"
    assert result_payload["text"] == repaired_report
    assert any(event_type == "final_repaired" for event_type, _kwargs in events)


def test_cron_delivery_withholds_email_body_leak_without_row_summary() -> None:
    email_body = (
        "Dear account holder, the following electronic deposit and associated "
        "transactions to your account have been completed. Log in to your account "
        "and visit the Activity tab for more information. Please note that it may "
        "not be available for immediate withdrawal until the deposit has been "
        "verified. Thanks for being a customer. "
        "Questions? Contact support for account help."
    )
    result_payload = {
        "text": (
            "I checked your email.\n"
            "A deposit is complete\n"
            "From: finance@example.test\n\n"
            f"{email_body}"
        ),
        "tool_results": [
            ToolResult(
                "email-read-1",
                "email_read",
                "completed",
                {
                    "message": {
                        "headers": {
                            "from": "finance@example.test",
                            "date": "Thu, 21 May 2026 07:12:06 +0000",
                            "subject": "A deposit is complete",
                        },
                        "body": email_body,
                    }
                },
            )
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied raw account message content instead of the scheduled-task report" in delivered
    assert "No verified deadline/action report was produced" in delivered
    assert "Safe summary from verified tool results" not in delivered
    assert "1. A deposit is complete" not in delivered
    assert "finance@example.test" not in delivered
    assert "Thu, 21 May 2026 07:12:06 +0000" not in delivered
    assert "Dear account holder" not in delivered
    assert "visit the Activity tab" not in delivered
    assert result_payload["cron_sensitive_tool_body_withheld"] is True
    assert result_payload["cron_sensitive_tool_summary_withheld"] is True
    assert "cron_sensitive_tool_summary_repaired" not in result_payload


def test_cron_delivery_withholds_email_metadata_inventory_without_row_summary() -> None:
    result_payload = {
        "raw_tool_payload_blocked": True,
        "text": (
            "The scheduled task checked email/account data, but raw message body text was withheld "
            "from the delivery.\n"
            "Messages checked:\n"
            "- Congratulations Example, you have been selected to earn points · American Express "
            "<americanexpress@example.test> · Sat, 23 May 2026 17:06:09 -0600\n"
            "- Your score changed · Credit Karma <notifications@example.test> · Fri, 22 May 2026 18:50:49 +0000"
        ),
        "tool_results": [
            ToolResult(
                "email-search-1",
                "email_search",
                "completed",
                {
                    "messages": [
                        {
                            "headers": {
                                "from": "American Express <americanexpress@example.test>",
                                "date": "Sat, 23 May 2026 17:06:09 -0600",
                                "subject": "Congratulations Example, you have been selected to earn points",
                            },
                        },
                        {
                            "headers": {
                                "from": "Credit Karma <notifications@example.test>",
                                "date": "Fri, 22 May 2026 18:50:49 +0000",
                                "subject": "Your score changed",
                            },
                        },
                    ],
                },
            )
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied account message metadata instead of the scheduled-task report" in delivered
    assert "No verified deadline/action report was produced" in delivered
    assert "Safe summary from verified tool results" not in delivered
    assert "Messages checked" not in delivered
    assert "Congratulations Example" not in delivered
    assert "americanexpress@example.test" not in delivered
    assert "Credit Karma" not in delivered
    assert "notifications@example.test" not in delivered
    assert result_payload["cron_sensitive_tool_metadata_withheld"] is True
    assert result_payload["cron_sensitive_tool_summary_withheld"] is True


def test_cron_delivery_withholds_email_preview_dump_without_row_summary() -> None:
    result_payload = {
        "text": (
            "✉️ I checked your email.\n\n"
            "**Your receipt from Sullivan Medical Midtown**\n"
            "From: Sullivan Medical Midtown <noreply@patient-message.com>\n"
            "Date: Tue, 09 Jun 2026 15:29:53 +0000\n\n"
            "Preview: Keep this receipt for your records Sullivan Medical Midtown Your receipt from "
            "Sullivan Medical Midtown Payment Receipt Thank you for your recent payment."
        ),
        "tool_results": [
            {
                "tool_name": "email_read",
                "status": "completed",
                "output": {
                    "message": {
                        "headers": {
                            "from": "Sullivan Medical Midtown <noreply@patient-message.com>",
                            "date": "Tue, 09 Jun 2026 15:29:53 +0000",
                            "subject": "Your receipt from Sullivan Medical Midtown",
                        },
                        "snippet": "Keep this receipt for your records Sullivan Medical Midtown Your receipt",
                    }
                },
            }
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied an email preview instead of the scheduled-task report" in delivered
    assert "No verified deadline/action report was produced" in delivered
    assert "Safe summary from verified tool results" not in delivered
    assert "1. Your receipt from Sullivan Medical Midtown" not in delivered
    assert "Sullivan Medical Midtown <noreply@patient-message.com>" not in delivered
    assert "Preview:" not in delivered
    assert "Keep this receipt" not in delivered
    assert result_payload["cron_sensitive_tool_preview_withheld"] is True
    assert result_payload["cron_sensitive_tool_summary_withheld"] is True
    assert "cron_sensitive_tool_summary_repaired" not in result_payload


def test_cron_delivery_withholds_calendar_connector_summary_dump_without_row_summary() -> None:
    result_payload = {
        "text": (
            "⏰ **MANUAL SCHEDULED TASK RUN: Email deadline triage**\n\n"
            "**📅 I found 4 matching calendar events**\n\n"
            "• **Upstate Trip** — 2026-06-14 - 2026-06-15\n"
            "• **Con Edison autopay: $186.72 deduction** — 2026-06-18 - 2026-06-19\n"
            "• **Appointment: Sullivan Medical Midtown with Scott Nelson, PA** — "
            "2026-06-09 11:00 - 2026-06-09 11:30"
        ),
        "tool_results": [
            {
                "tool_name": "calendar_list",
                "status": "completed",
                "output": {
                    "results": [
                        {
                            "summary": "Upstate Trip",
                            "start": {"date": "2026-06-14"},
                            "end": {"date": "2026-06-15"},
                        },
                        {
                            "summary": "Con Edison autopay: $186.72 deduction",
                            "start": {"date": "2026-06-18"},
                            "end": {"date": "2026-06-19"},
                        },
                        {
                            "summary": "Appointment: Sullivan Medical Midtown with Scott Nelson, PA",
                            "start": {"dateTime": "2026-06-09T11:00:00-04:00"},
                            "end": {"dateTime": "2026-06-09T11:30:00-04:00"},
                            "location": "124 W 36TH ST FL 4, New York, NY 10018-6915",
                        },
                    ]
                },
            }
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied a connector result list instead of the scheduled-task report" in delivered
    assert "No verified deadline/action report was produced" in delivered
    assert "Safe summary from verified tool results" not in delivered
    assert "1. Upstate Trip" not in delivered
    assert "2. Con Edison autopay: $186.72 deduction" not in delivered
    assert "3. Appointment: Sullivan Medical Midtown with Scott Nelson, PA" not in delivered
    assert "I found 4 matching calendar events" not in delivered
    assert "MANUAL SCHEDULED TASK RUN" not in delivered
    assert result_payload["cron_account_tool_summary_withheld"] is True
    assert result_payload["cron_account_tool_summary_withheld_without_repair"] is True
    assert "cron_account_tool_summary_repaired" not in result_payload


def test_cron_delivery_withholds_file_search_inventory_dump_without_report_summary() -> None:
    result_payload = {
        "text": (
            "Files I found\n\n"
            "• I found 49 matching files: 2026-05-09-nullion-social-github-traffic-report.html, "
            "buildcurrentnullionreport.py, buildnullionmonitor.py, buildnullionmonitorreport.py, "
            "buildnullionreportcurrent.py, and 44 more. I have not read the file contents yet."
        ),
        "tool_results": [
            {
                "tool_name": "file_search",
                "status": "completed",
                "output": {
                    "matches": [
                        "/workspace/artifacts/2026-05-09-nullion-social-github-traffic-report.html",
                        "/workspace/artifacts/buildcurrentnullionreport.py",
                        "/workspace/artifacts/buildnullionmonitor.py",
                        "/workspace/artifacts/buildnullionmonitorreport.py",
                        "/workspace/artifacts/buildnullionreportcurrent.py",
                    ]
                },
            }
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied a file-search inventory instead of a verified report" in delivered
    assert "No verified scheduled-task result was produced" in delivered
    assert "Files I found" not in delivered
    assert "2026-05-09-nullion-social-github-traffic-report.html" not in delivered
    assert "buildcurrentnullionreport.py" not in delivered
    assert result_payload["cron_file_inventory_withheld"] is True


def test_cron_delivery_keeps_concise_email_summary_when_body_not_leaked() -> None:
    email_body = "Sensitive account message body. " * 20
    result_payload = {
        "text": "Checked email: one payment confirmation was found. No calendar action was needed.",
        "tool_results": [
            {
                "tool_name": "email_read",
                "status": "completed",
                "output": {
                    "message": {
                        "headers": {"from": "billing@example.test", "subject": "Payment confirmation"},
                        "body": email_body,
                    }
                },
            }
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == result_payload["text"]
    assert "cron_sensitive_tool_body_withheld" not in result_payload


def test_cron_delivery_withholds_skill_pack_doc_leak_without_row_summary() -> None:
    reference_text = "# Connector Reference\n\n" + ("Use these internal endpoint notes. " * 20)
    result_payload = {
        "text": reference_text,
        "tool_results": [
            ToolResult(
                "skill-read-1",
                "skill_pack_read",
                "completed",
                {"path": "README.md", "text": reference_text},
            ),
            ToolResult(
                "email-read-1",
                "email_read",
                "completed",
                {
                    "message": {
                        "headers": {
                            "from": "billing@example.test",
                            "date": "Sun, 07 Jun 2026 12:15:00 +0000",
                            "subject": "Payment deadline tomorrow",
                        },
                        "snippet": "Payment deadline reminder.",
                    }
                },
            ),
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload)

    assert "copied internal connector/tool documentation" in delivered
    assert "Safe summary from verified tool results" not in delivered
    assert "1. Payment deadline tomorrow" not in delivered
    assert "billing@example.test" not in delivered
    assert "Sun, 07 Jun 2026 12:15:00 +0000" not in delivered
    assert "# Connector Reference" not in delivered
    assert "Use these internal endpoint notes" not in delivered
    assert result_payload["cron_internal_reference_output_withheld"] is True
    assert result_payload["cron_internal_reference_output_summary_withheld"] is True
    assert "cron_internal_reference_output_summary_repaired" not in result_payload


def test_cron_delivery_blocks_internal_reference_tool_output_leak() -> None:
    reference_text = "# Connector Routing Reference\n\n" + ("Use these internal endpoint notes. " * 20)
    result_payload = {
        "text": reference_text,
        "tool_results": [
            {
                "tool_name": "skill_pack_read",
                "status": "completed",
                "output": {"path": "references/service/README.md", "text": reference_text},
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=reference_text)
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_preserves_internal_reference_reason_after_safe_repair() -> None:
    result_payload = {
        "text": "The scheduled task withheld internal connector/tool documentation.",
        "cron_internal_reference_output_withheld": True,
        "cron_internal_reference_output_summary_withheld": True,
        "tool_results": [
            {
                "tool_name": "skill_pack_read",
                "status": "completed",
                "output": {"path": "references/service/README.md", "text": "internal reference text"},
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(
            result_payload,
            artifacts=None,
            text=str(result_payload["text"]),
        )
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_blocks_raw_browser_extract_text_leak() -> None:
    page_text = (
        "Skip to main content\n"
        "Accessibility help\n"
        "AI Mode\n"
        "All\n"
        "Images\n"
        "Videos\n"
        "News\n"
        "Shopping\n"
        "Forums\n"
        "More\n"
        "Tools\n"
        "Search Results\n"
        "Web results\n"
        "Jimmy Eat World - New York - The Rooftop at Pier 17\n\n"
        "AXS\n"
        "https://www.axs.com › events › jimmy-eat-world-tickets\n"
        "Jun 17, 2026 — Get tickets for Jimmy Eat World with Thrice, "
        "Girls Against Boys at The Rooftop at Pier 17 in New York on Wed, "
        "17 June 2026 - 06:00 PM at AXS.\n"
        "AXS Official Resale - AXS US\n\n"
        "AXS\n"
        "https://www.axs.com › jimmy-eat-world-tickets › promos\n"
        "Feb 13, 2026 — VIP Packages, promotions and special offers for "
        "Jimmy Eat World at The Rooftop at Pier 17 in New York."
    )
    result_payload = {
        "text": page_text,
        "tool_results": [
            ToolResult(
                "browser-1",
                "browser_extract_text",
                "completed",
                {"text": page_text, "url": "https://www.google.com/search?q=jimmy+eat+world+pier+17"},
            )
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=page_text)
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_blocks_source_points_only_browser_summary() -> None:
    page_text = (
        "TickPick partners with leading teams, leagues, and organizations to bring "
        "fans the best ticket experiences. From official partnerships to authorized "
        "marketplaces, we're committed to providing access to the biggest events.\n"
        "Ticket Brokers\n"
        "Don Toliver"
    )
    source_points = (
        "**Relevant source points**\n\n"
        "Source: https://www.google.com/search?q=Vivid+Seats+Jimmy+Eat+World+Pier+17\n\n"
        "- TickPick partners with leading teams, leagues, and organizations to bring "
        "fans the best ticket experiences.\n"
        "- Ticket Brokers\n"
        "- Don Toliver"
    )
    result_payload = {
        "text": source_points,
        "tool_results": [
            ToolResult(
                "browser-1",
                "browser_extract_text",
                "completed",
                {
                    "text": page_text,
                    "url": "https://www.google.com/search?q=Vivid+Seats+Jimmy+Eat+World+Pier+17",
                },
            )
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=source_points)
        == "cron_run_source_points_only"
    )
    assert result_payload["cron_source_points_only_withheld"] is True


def test_cron_delivery_allows_answer_with_source_points_appendix() -> None:
    page_text = (
        "LOWEST PRICE\n"
        "SECTION\n"
        "We're All In: Prices include fees (before taxes).\n"
        "Sec GENERAL ADMISSION\n"
        "Resale Ticket\n"
        "$182.45\n"
        "Mobile Entry"
    )
    report = (
        "No under-$100 ticket was found. Lowest verified listing is $182.45 before taxes.\n\n"
        "**Relevant source points**\n\n"
        "Source: https://www.ticketmaster.example/event\n\n"
        "- Ticketmaster showed a General Admission resale listing at $182.45."
    )
    result_payload = {
        "text": report,
        "tool_results": [
            ToolResult(
                "browser-1",
                "browser_extract_text",
                "completed",
                {
                    "text": page_text,
                    "url": "https://www.ticketmaster.example/event",
                },
            )
        ],
    }

    assert cron_structured_result_block_reason(result_payload, artifacts=None, text=report) is None
    assert "cron_source_points_only_withheld" not in result_payload


def test_cron_delivery_blocks_short_browser_ui_extract_text_leak() -> None:
    page_text = (
        "Best\n\n"
        "26 min\n\n"
        "1h 4m\n\n"
        "2h 49m\n\n"
        "52 min\n\n"
        "Add destination\n\n"
        "Search along the route...\n\n"
        "Gas\n\n"
        "EV charging\n\n"
        "Hotels\n\n"
        "Options"
    )
    result_payload = {
        "text": page_text,
        "tool_results": [
            ToolResult(
                "browser-1",
                "browser_extract_text",
                "completed",
                {"text": page_text, "url": "https://maps.example.test/route"},
            )
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=page_text)
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_blocks_skill_pack_prompt_leak_without_tool_result() -> None:
    prompt_text = (
        "Skill pack: vendor/example-skill\n"
        "Use when the active workspace has an API gateway connection. "
        "Use skill_pack_read for detailed installed-pack docs when a structured tool plan needs service-specific steps."
    )
    result_payload = {
        "text": prompt_text,
        "tool_results": [],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=prompt_text)
        == "cron_run_malformed_delivery_text"
    )


def test_cron_delivery_blocks_internal_action_receipt_leak() -> None:
    receipt_text = "Completed action: terminal_exec."
    result_payload = {
        "text": receipt_text,
        "tool_results": [
            {
                "tool_name": "terminal_exec",
                "status": "completed",
                "output": {
                    "exit_code": 0,
                    "stdout": "raw command output",
                    "action_receipt": {
                        "type": "action_receipt",
                        "summary": receipt_text,
                        "details": [],
                    },
                },
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=receipt_text)
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_block_reason_uses_shared_repaired_raw_payload_state() -> None:
    result_payload = {
        "text": "Email deadline triage complete.\n\nNo new calendar event needed.",
        "tool_results": [
            {
                "tool_name": "email_search",
                "status": "completed",
                "output": {"count": 2},
            }
        ],
        "raw_tool_payload_blocked": True,
        "cron_raw_tool_payload_repaired": True,
    }

    assert cron_delivery_block_reason(result_payload, result_payload["text"], artifacts=None) is None

    result_payload["cron_raw_tool_payload_repaired"] = False

    assert (
        cron_delivery_block_reason(result_payload, result_payload["text"], artifacts=None)
        == "cron_run_raw_tool_payload"
    )


def test_cron_delivery_blocks_file_patch_action_receipt_leak() -> None:
    result_payload = {
        "text": "Completed action: file_patch.\n\nDetails:\n- Path: files/make_ptown_report.py.",
        "tool_results": [
            {
                "tool_name": "file_patch",
                "status": "completed",
                "output": {
                    "path": "files/make_ptown_report.py",
                    "action_receipt": {
                        "type": "action_receipt",
                        "summary": "Completed action: file_patch.",
                        "details": ["Path: files/make_ptown_report.py."],
                    },
                },
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(
            result_payload,
            artifacts=None,
            text=str(result_payload["text"]),
        )
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_blocks_empty_tool_scope_result_leak() -> None:
    scope_message = "No registered tools matched the requested capability scope."
    result_payload = {
        "text": scope_message,
        "tool_results": [
            {
                "tool_name": "request_tool_scope",
                "status": "completed",
                "output": {
                    "scope_requested": True,
                    "capabilities": ["tracking"],
                    "available_tools": [],
                    "message": scope_message,
                },
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=scope_message)
        == "cron_run_internal_tool_output_leaked"
    )


def test_cron_delivery_does_not_count_scope_request_as_task_evidence() -> None:
    result_payload = {
        "text": "I could not verify the scheduled check.",
        "tool_results": [
            {
                "tool_name": "request_tool_scope",
                "status": "completed",
                "output": {
                    "scope_requested": True,
                    "capabilities": ["web"],
                    "available_tools": [],
                    "message": "No registered tools matched the requested capability scope.",
                },
            }
        ],
    }

    assert (
        cron_structured_result_block_reason(result_payload, artifacts=None, text=result_payload["text"])
        == "cron_run_tool_scope_unavailable"
    )


def test_cron_delivery_allows_completed_non_scheduler_tool_evidence() -> None:
    result_payload = {
        "text": "report",
        "tool_results": [
            {
                "tool_name": "web_search",
                "status": "completed",
                "output": {"summary": "report"},
            }
        ],
    }

    assert cron_structured_result_block_reason(result_payload, artifacts=None) is None


def test_cron_run_delivery_workflow_keeps_state_update_summary_without_state_media() -> None:
    state_path = "/workspace/files/payment_reminder_tracker.json"
    artifact_path = "/workspace/artifacts/payment_reminder_tracker.json"

    result_payload = {
        "final_text": f"saved state\n\nMEDIA:{artifact_path}\n\n新しい期限があります",
        "artifacts": [{"path": artifact_path}],
        "tool_results": [
            {"tool_name": "file_write", "output": {"path": state_path}},
            {"tool_name": "file_write", "output": {"path": artifact_path}},
        ],
    }

    assert cron_delivery_text_from_result(result_payload) == "新しい期限があります"


def test_cron_run_delivery_workflow_allows_silent_no_change_runs() -> None:
    events: list[str] = []
    cleared: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {},
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: (_ for _ in ()).throw(AssertionError("should not send")),
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "silent"
    assert "cron.delivery.silent" in events
    assert cleared == ["telegram:42"]


def test_typed_verified_matches_only_policy_keeps_unfinished_check_failed() -> None:
    calls: list[dict[str, object]] = []
    stages: list[tuple[str, dict[str, object]]] = []

    class DecisionModel:
        def create(self, **kwargs):  # noqa: ANN003
            calls.append(kwargs)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": '{"decision":"silent","reason":"no_verified_match"}',
                    }
                ]
            }

    result = {
        "text": "No se verificaron anuncios nuevos.",
        "reached_iteration_limit": True,
        "response_fulfilled": False,
        "tool_results": [
            ToolResult(
                "extract",
                "browser_extract_text",
                "completed",
                {"text": "Próximas fechas\n© 2026 Sitio de eventos"},
            )
        ],
    }
    job = Job(
        task="Avisa únicamente sobre coincidencias verificadas; no envíes un informe vacío.",
        artifact_delivery_options={"alert_delivery_mode": "verified_matches_only"},
    )

    _apply_cron_alert_delivery_policy(
        job,
        result,
        model_client=DecisionModel(),
        record_preflight=lambda stage, **payload: stages.append((stage, payload)),
    )

    assert result["cron_alert_delivery_decision"] == "silent"
    assert result["cron_alert_delivery_reason"] == "insufficient_evidence"
    assert result["text"] == "No se verificaron anuncios nuevos."
    assert result["reached_iteration_limit"] is True
    assert result["response_fulfilled"] is False
    assert result["cron_report_failed"] is True
    assert not calls and not stages


def test_typed_verified_matches_only_policy_accepts_a_completed_negative():
    class Model:
        def create(self, **kwargs):
            return {"content": [{"type": "text", "text": '{"decision":"silent","reason":"no_verified_match"}'}]}
    result = {"text": "指定日の条件を確認済み。一致なし。", "response_fulfilled": True,
              "tool_results": [ToolResult("extract", "browser_extract_text", "completed",
                                           {"text": "Selected date: 2026-10-16. No qualifying records."})]}
    job = Job(task="条件一致だけ通知。", artifact_delivery_options={"alert_delivery_mode": "verified_matches_only"})
    _apply_cron_alert_delivery_policy(job, result, model_client=Model())
    assert result["cron_alert_delivery_decision"] == "silent"
    assert result["cron_alert_delivery_reason"] == "no_verified_match"
    assert result["response_fulfilled"] is True
    assert result["text"] == ""


@pytest.mark.parametrize("manual", [False, True])
def test_explicit_alert_policy_silence_has_a_receipt_only_for_manual_runs(manual) -> None:
    events: list[str] = []
    sent: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {
            "text": "",
            "response_fulfilled": True,
            "cron_alert_delivery_decision": "silent",
            "cron_alert_delivery_reason": "no_verified_match",
            "tool_results": [
                ToolResult(
                    "extract",
                    "browser_extract_text",
                    "completed",
                    {"text": "Upcoming Dates\n© 2026 Example Events"},
                )
            ],
        },
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
        silent_delivery_text=manual_cron_silent_delivery_text if manual else None,
    )

    result = run_cron_delivery_workflow(
        Job(delivery_channel="telegram"),
        label="Manual scheduled task run",
        callbacks=callbacks,
    )

    assert result["cron_delivery_status"] == ("sent" if manual else "silent")
    assert bool(result.get("cron_silent_result_replaced")) is manual
    assert ("cron.delivery.silent" in events) is not manual
    assert bool(sent) is manual
    if manual:
        assert "There was nothing new to report." in sent[0]


def test_manual_cron_no_change_run_delivers_terminal_message() -> None:
    events: list[str] = []
    sent: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {},
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
        silent_delivery_text=manual_cron_silent_delivery_text,
    )

    result = run_cron_delivery_workflow(
        Job(delivery_channel="telegram", name="Peak Design shipment tracker"),
        label="Manual scheduled task run",
        callbacks=callbacks,
    )

    assert result["cron_delivery_status"] == "sent"
    assert result["cron_silent_result_replaced"] is True
    assert "cron.delivery.silent" not in events
    assert "cron.delivery.sent" in events
    assert sent == [
        "⏰ **MANUAL SCHEDULED TASK RUN: Peak Design shipment tracker**\n\nThere was nothing new to report."
    ]


def test_manual_cron_deferred_receipt_does_not_duplicate_working_ack() -> None:
    receipt = manual_cron_deferred_receipt()

    assert receipt["cron_delivery_status"] == "deferred"
    assert receipt["text"] == "Manual scheduled task run started. The result will be delivered when ready."


def test_manual_cron_terminal_status_uses_human_tool_evidence_failure_reason() -> None:
    text = manual_cron_terminal_status_text(
        Job(name="Email deadline triage"),
        label="Manual scheduled task run",
        result={
            "cron_delivery_status": "failed",
            "reason": "cron_run_without_completed_tool_evidence",
        },
    )

    assert "cron_run_without_completed_tool_evidence" not in text
    assert "did not complete a data-gathering tool" in text


def test_manual_cron_terminal_status_surfaces_account_connection_failure() -> None:
    result = {
        "tool_results": [
            ToolResult(
                invocation_id="tool-1",
                tool_name="email_search",
                status="failed",
                output={"query": "deadline"},
                error="custom_api_provider requires CUSTOM_TOKEN or NULLION_CUSTOM_API_TOKEN",
            )
        ],
        "artifacts": [],
    }

    reason = cron_structured_result_block_reason(result, artifacts=[])
    text = manual_cron_terminal_status_text(
        Job(name="Email deadline triage"),
        label="Manual scheduled task run",
        result={
            "cron_delivery_status": "failed",
            "reason": reason,
            "error": result["error"],
        },
    )

    assert reason == "cron_run_account_connection_unavailable"
    assert "cron_run_without_completed_tool_evidence" not in text
    assert "did not complete a data-gathering tool" not in text
    assert "custom_api_provider is missing required connection configuration" in text


def test_cron_agent_history_includes_connection_context_for_structured_account_tools(monkeypatch) -> None:
    captured: list[bool] = []

    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: "/tmp/artifacts")
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    monkeypatch.setattr("nullion.runtime_config.format_runtime_config_for_prompt", lambda model_client: "")
    monkeypatch.setattr("nullion.skill_pack_catalog.skill_pack_access_prompt", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        "nullion.skill_pack_installer.format_cached_enabled_skill_pack_index_for_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr("nullion.system_context.build_system_context_snapshot", lambda tool_registry: object())
    monkeypatch.setattr("nullion.system_context.format_compact_system_context_for_prompt", lambda snapshot: "")
    monkeypatch.setattr("nullion.workspace_storage.format_workspace_storage_for_prompt", lambda principal_id: "")

    def fake_connections_text(*, principal_id, include_external_connectors):  # noqa: ANN001
        _ = principal_id
        captured.append(include_external_connectors)
        return "Configured workspace connections\n- typed_tools=email_search,email_read"

    monkeypatch.setattr("nullion.connections.format_workspace_connections_for_prompt", fake_connections_text)

    history = cron_agent_history(
        object(),
        SimpleNamespace(enabled_skill_packs=()),
        model_client=None,
        principal_id="telegram:100001",
        tool_registry=object(),
        include_connector_context=False,
        include_structured_connection_context=True,
    )

    assert captured == [True]
    assert any("typed_tools=email_search,email_read" in block["content"][0]["text"] for block in history)


def test_cron_agent_iteration_budget_is_separate_from_chat_default(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "12")
    monkeypatch.delenv("NULLION_CRON_AGENT_MAX_ITERATIONS", raising=False)

    assert default_cron_agent_max_iterations() == 32

    monkeypatch.setenv("NULLION_CRON_AGENT_MAX_ITERATIONS", "36")
    assert default_cron_agent_max_iterations() == 36

    monkeypatch.setenv("NULLION_CRON_AGENT_MAX_ITERATIONS", "200")
    assert default_cron_agent_max_iterations() == 40


def test_run_single_agent_cron_turn_passes_cron_iteration_budget(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "12")
    monkeypatch.delenv("NULLION_CRON_AGENT_MAX_ITERATIONS", raising=False)
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    monkeypatch.setattr("nullion.runtime_config.format_runtime_config_for_prompt", lambda model_client: "")
    monkeypatch.setattr("nullion.skill_pack_catalog.skill_pack_access_prompt", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        "nullion.skill_pack_installer.format_cached_enabled_skill_pack_index_for_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr("nullion.system_context.build_system_context_snapshot", lambda tool_registry: object())
    monkeypatch.setattr("nullion.system_context.format_compact_system_context_for_prompt", lambda snapshot: "")
    monkeypatch.setattr("nullion.workspace_storage.format_workspace_storage_for_prompt", lambda principal_id: "")
    monkeypatch.setattr(
        "nullion.cron_execution_tools.build_cron_connector_scope_decision",
        lambda **kwargs: SimpleNamespace(allow_connector_tools=False, provider_ids=()),
    )
    monkeypatch.setattr("nullion.cron_execution_tools.structured_cron_passthrough_tool_names", lambda registry: ())
    captured: dict[str, object] = {}

    class FakeOrchestrator:
        model_client = object()

        def run_turn(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return SimpleNamespace(
                final_text="Cron complete.",
                tool_results=[],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    result = run_single_agent_cron_turn(
        Job(
            name="Artifact workflow",
            artifact_delivery_options={
                "requires_attachment_delivery": True,
                "required_artifact_extensions": ["html"],
                "artifact_focus_min_source_results": 18,
                "required_artifact_content_tokens": ["MSFT", "NVDA"],
            },
        ),
        "telegram:100001",
        label="Scheduled task",
        orchestrator=FakeOrchestrator(),
        runtime=SimpleNamespace(store=None),
        tool_registry=ToolRegistry(),
        settings=SimpleNamespace(enabled_skill_packs=()),
    )

    assert result["reason"] == "cron_required_artifact_missing"
    assert result["cron_failure_notice"] is True
    assert result["response_fulfilled"] is False
    assert captured["max_iterations"] == 32
    assert captured["tool_flow_context"]["scheduled_task_run"] is True
    assert captured["tool_flow_context"]["requires_artifact_delivery"] is True
    assert captured["tool_flow_context"]["required_artifact_extensions"] == [".html"]
    assert captured["tool_flow_context"]["artifact_focus_min_source_results"] == 18
    assert captured["tool_flow_context"]["required_artifact_content_tokens"] == ["MSFT", "NVDA"]
    assert "A text summary cannot replace that artifact" in captured["user_message"]
    assert "after at least 18 source-tool results" in captured["user_message"]
    assert '["MSFT", "NVDA"]' in captured["user_message"]


def test_structured_html_timeout_report_is_complete_and_evidence_backed() -> None:
    report = _cron_structured_html_report(
        title="Post-market report",
        required_tokens=("MSFT", "NVDA"),
        tool_results=[
            ToolResult(
                "quotes-1",
                "market_quote",
                "completed",
                {
                    "quotes": [
                        {
                            "symbol": "MSFT",
                            "name": "Microsoft Corporation",
                            "price": 403.45,
                            "change": 2.5,
                            "change_percent": 0.62,
                            "currency": "USD",
                            "source_url": "https://example.test/quote/MSFT",
                        },
                        {
                            "symbol": "NVDA",
                            "name": "NVIDIA Corporation",
                            "price": 181.3,
                            "change": -1.2,
                            "change_percent": -0.66,
                            "currency": "USD",
                            "source_url": "https://example.test/quote/NVDA",
                        },
                    ]
                },
            )
        ],
    )

    assert report is not None
    assert report.startswith("<!doctype html>")
    assert report.endswith("</body></html>")
    assert report.count('<article class="card">') == 2
    assert "403.45 USD" in report
    assert "NVIDIA Corporation" in report
    assert "<script" not in report
    assert "<img" not in report
    assert missing_required_artifact_content_tokens(
        report,
        suffix=".html",
        required_tokens=("MSFT", "NVDA"),
    ) == ()


def test_structured_html_timeout_report_refuses_low_coverage_source_boilerplate() -> None:
    report = _cron_structured_html_report(
        title="Public awareness report",
        required_tokens=("GitHub Traffic Trend", "Reddit", "YouTube", "Source freshness"),
        tool_results=[
            ToolResult(
                "browser-1",
                "browser_extract_text",
                "completed",
                {
                    "title": "Reddit search",
                    "url": "https://example.test/search",
                    "text": "All Regions Argentina Australia Austria Belgium Canada Reddit search",
                },
            )
        ],
    )

    assert report is None


def test_structured_html_continuation_stops_when_stalled_unsafe_or_exhausted() -> None:
    from nullion.cron_delivery import _instruction_aware_cron_html
    from nullion.model_clients import ModelClientTimeoutError

    prefix = '<!doctype html><html><body><p>'
    for fragments, expected_timeouts in (
        ([prefix, ''], [120, 180]),
        ([prefix, 'A', 'B'], [120, 180, 90]),
        ([prefix + '<script>', ''], [120, 180]),
    ):
        calls = []

        class Client:
            def create(self, **kwargs):
                calls.append(kwargs['timeout'])
                assert len(calls) <= len(fragments), 'Unbounded continuation'
                if '<script>' in fragments[0] and len(calls) == 2:
                    assert len(kwargs['messages'][0]['content']) == 1
                kwargs['text_delta_callback'](fragments[len(calls) - 1])
                raise ModelClientTimeoutError('Interrupted document stream')

        result = {'tool_results': [ToolResult('q', 'market_quote', 'completed',
                                             {'symbol': 'AAA', 'price': 20})]}
        assert _instruction_aware_cron_html(Job(task='価格を表示'), result,
                                           model_client=Client()) is None
        assert calls == expected_timeouts


def test_run_single_agent_cron_turn_recovers_required_html_after_model_timeout(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    monkeypatch.setattr("nullion.runtime_config.format_runtime_config_for_prompt", lambda model_client: "")
    monkeypatch.setattr("nullion.skill_pack_catalog.skill_pack_access_prompt", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        "nullion.skill_pack_installer.format_cached_enabled_skill_pack_index_for_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr("nullion.system_context.build_system_context_snapshot", lambda tool_registry: object())
    monkeypatch.setattr("nullion.system_context.format_compact_system_context_for_prompt", lambda snapshot: "")
    monkeypatch.setattr("nullion.workspace_storage.format_workspace_storage_for_prompt", lambda principal_id: "")
    monkeypatch.setattr(
        "nullion.cron_execution_tools.build_cron_connector_scope_decision",
        lambda **kwargs: SimpleNamespace(allow_connector_tools=False, provider_ids=()),
    )
    monkeypatch.setattr("nullion.cron_execution_tools.structured_cron_passthrough_tool_names", lambda registry: ())

    class RecoveryClient:
        def create(self, **kwargs):
            payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
            assert payload["instructions"] == "make the brief"
            return {"content": [{"type": "text", "text": json.dumps({
                "requirements_satisfied": True,
                "html": "<!doctype html><html><body>MSFT 403.45 USD; NVDA 181.3 USD</body></html>",
            })}]}

    class FakeOrchestrator:
        model_client = RecoveryClient()

        def run_turn(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            return SimpleNamespace(
                final_text="I couldn't complete this because the model timed out.",
                tool_results=[
                    ToolResult(
                        "quotes-1",
                        "market_quote",
                        "completed",
                        {
                            "quotes": [
                                {"symbol": "MSFT", "name": "Microsoft", "price": 403.45, "currency": "USD"},
                                {"symbol": "NVDA", "name": "NVIDIA", "price": 181.3, "currency": "USD"},
                            ]
                        },
                    )
                ],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
                response_fulfilled=False,
                model_timed_out=True,
                artifact_delivery_required=True,
                artifact_delivery_satisfied=False,
                required_artifact_extensions=(".html",),
            )

    result = run_single_agent_cron_turn(
        Job(
            name="Post-market report",
            artifact_delivery_options={
                "requires_attachment_delivery": True,
                "required_artifact_extensions": ["html"],
                "required_artifact_content_tokens": ["MSFT", "NVDA"],
                "deliver_required_extensions_only": True,
            },
        ),
        "telegram:100001",
        label="Scheduled task",
        orchestrator=FakeOrchestrator(),
        runtime=SimpleNamespace(store=None),
        tool_registry=ToolRegistry(),
        settings=SimpleNamespace(enabled_skill_packs=()),
    )

    assert result["cron_structured_artifact_recovered"] is True
    assert result["cron_agent_model_timed_out_before_structured_artifact_recovery"] is True
    assert result["model_timed_out"] is False
    assert result["response_fulfilled"] is True
    assert result["artifact_delivery_satisfied"] is True
    assert result["text"] == "Report attached."
    paths = cron_delivery_artifact_paths_from_result(result)
    assert len(paths) == 1
    assert Path(paths[0]).suffix == ".html"
    assert Path(paths[0]).is_file()
    delivered = cron_delivery_text_from_result(result)
    assert delivered.count("MEDIA:") == 1
    assert "model timed out" not in delivered


def test_structured_html_terminal_limit_accepts_existing_complete_report(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    existing_path = tmp_path / "artifacts" / "complete-report.html"
    existing_path.parent.mkdir()
    existing_path.write_text(
        "<!doctype html><html><head><title>Report</title></head>"
        "<body><main><h1>GitHub Traffic Trend</h1><section>Reddit</section></main></body></html>",
        encoding="utf-8",
    )
    job = Job(
        name="Required report",
        task="",  # Legacy metadata-only contract; saved instructions are covered separately.
        artifact_delivery_options={
            "requires_attachment_delivery": True,
            "required_artifact_extensions": [".html"],
            "required_artifact_content_tokens": ["GitHub Traffic Trend", "Reddit"],
        },
    )
    result = {
        "model_timed_out": False,
        "reached_iteration_limit": True,
        "artifact_delivery_satisfied": True,
        "tool_results": [
            ToolResult(
                "write-1",
                "file_write",
                "completed",
                {"path": str(existing_path), "artifact_path": str(existing_path)},
            )
        ],
        "artifacts": [str(existing_path), str(tmp_path / "scratch.txt")],
    }

    assert _recover_typed_html_artifact_after_terminal_limit(job, "telegram:100001", result) is True
    assert result["cron_typed_artifact_accepted_after_terminal_limit"] is True
    assert result["cron_agent_iteration_limit_before_structured_artifact_recovery"] is True
    assert result["reached_iteration_limit"] is False
    assert result["artifact_delivery_satisfied"] is True
    assert result["artifacts"] == [str(existing_path)]
    assert cron_delivery_artifact_paths_from_result(result) == (str(existing_path),)


def test_structured_html_timeout_recovery_refuses_unmatched_evidence(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    job = Job(
        name="Required report",
        artifact_delivery_options={
            "requires_attachment_delivery": True,
            "required_artifact_extensions": [".html"],
            "required_artifact_content_tokens": ["MSFT"],
        },
    )
    result = {
        "model_timed_out": True,
        "artifact_delivery_satisfied": False,
        "tool_results": [
            ToolResult(
                "quotes-1",
                "market_quote",
                "completed",
                {"quotes": [{"symbol": "NVDA", "price": 181.3}]},
            )
        ],
        "artifacts": [],
    }

    assert _recover_typed_html_artifact_after_terminal_limit(job, "telegram:100001", result) is False
    assert result["model_timed_out"] is True
    assert result["artifacts"] == []
    assert list(tmp_path.glob("*.html")) == []


def test_run_single_agent_cron_turn_repairs_email_preview_dump_before_delivery(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "12")
    monkeypatch.delenv("NULLION_CRON_AGENT_MAX_ITERATIONS", raising=False)
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    monkeypatch.setattr("nullion.runtime_config.format_runtime_config_for_prompt", lambda model_client: "")
    monkeypatch.setattr("nullion.skill_pack_catalog.skill_pack_access_prompt", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        "nullion.skill_pack_installer.format_cached_enabled_skill_pack_index_for_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr("nullion.system_context.build_system_context_snapshot", lambda tool_registry: object())
    monkeypatch.setattr("nullion.system_context.format_compact_system_context_for_prompt", lambda snapshot: "")
    monkeypatch.setattr("nullion.workspace_storage.format_workspace_storage_for_prompt", lambda principal_id: "")
    monkeypatch.setattr(
        "nullion.cron_execution_tools.build_cron_connector_scope_decision",
        lambda **kwargs: SimpleNamespace(allow_connector_tools=True, provider_ids=("gmail",)),
    )
    monkeypatch.setattr("nullion.cron_execution_tools.structured_cron_passthrough_tool_names", lambda registry: ())

    repaired_report = (
        "📬 **Urgent deadlines/payments due soon**\n\n"
        "- None verified from the receipt email.\n\n"
        "📌 **Items needing review**\n\n"
        "- Sullivan Medical Midtown sent a payment receipt on Jun 9, 2026; no deadline or next action was verified."
    )

    class FakeModelClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            return {"content": [{"type": "text", "text": json.dumps({"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"], "report": repaired_report})}]}

    email_body = (
        "Keep this receipt for your records Sullivan Medical Midtown Your receipt from Sullivan Medical Midtown "
        "Payment Receipt Thank you for your recent payment with Todd Mcniff Md, Pc on 06/09/2026."
    )
    tool_result = ToolResult(
        "email-read-1",
        "email_read",
        "completed",
        {
            "message": {
                "headers": {
                    "from": "Sullivan Medical Midtown <noreply@patient-message.com>",
                    "date": "Tue, 09 Jun 2026 15:29:53 +0000",
                    "subject": "Your receipt from Sullivan Medical Midtown",
                },
                "snippet": "Keep this receipt for your records Sullivan Medical Midtown Your receipt",
                "body": email_body,
            }
        },
    )
    bad_final = (
        "✉️ I checked your email.\n\n"
        "**Your receipt from Sullivan Medical Midtown**\n"
        "From: Sullivan Medical Midtown <noreply@patient-message.com>\n"
        "Date: Tue, 09 Jun 2026 15:29:53 +0000\n\n"
        "Preview: Keep this receipt for your records Sullivan Medical Midtown Your receipt from "
        "Sullivan Medical Midtown"
    )
    model_client = FakeModelClient()

    class FakeOrchestrator:
        def __init__(self) -> None:
            self.model_client = model_client

        def run_turn(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            return SimpleNamespace(
                final_text=bad_final,
                tool_results=[tool_result],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    events: list[tuple[str, dict[str, object]]] = []

    result = run_single_agent_cron_turn(
        Job(
            name="Email deadline triage",
            task="Report urgent deadlines/payments due soon and items needing review.",
        ),
        "telegram:100001",
        label="Manual scheduled task run",
        orchestrator=FakeOrchestrator(),
        runtime=SimpleNamespace(store=None),
        tool_registry=ToolRegistry(),
        settings=SimpleNamespace(enabled_skill_packs=()),
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
    )

    assert result["cron_agent_final_repaired"] is True
    assert result["cron_agent_final_repair_reason"] == "account_preview_dumped"
    assert result["text"] == repaired_report
    assert model_client.calls
    repair_prompt = model_client.calls[0]["messages"][0]["content"][0]["text"]
    assert "Scheduled task instructions" in repair_prompt
    assert "Verified compact tool evidence" in repair_prompt

    delivered = cron_delivery_text_from_result(result)

    assert "copied an email preview instead of the scheduled-task report" not in delivered
    assert "Urgent deadlines/payments due soon" in delivered
    assert "no deadline or next action was verified" in delivered
    assert any(kwargs.get("stage") == "final_repaired" for _event_type, kwargs in events)


def test_run_single_agent_cron_turn_repairs_structured_evidence_fallback_before_delivery(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("NULLION_AGENT_TURN_MAX_ITERATIONS", "12")
    monkeypatch.delenv("NULLION_CRON_AGENT_MAX_ITERATIONS", raising=False)
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda principal_id: str(tmp_path))
    monkeypatch.setattr("nullion.builder_capabilities.format_installed_dependency_context", lambda runtime: "")
    monkeypatch.setattr("nullion.runtime_config.format_runtime_config_for_prompt", lambda model_client: "")
    monkeypatch.setattr("nullion.skill_pack_catalog.skill_pack_access_prompt", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        "nullion.skill_pack_installer.format_cached_enabled_skill_pack_index_for_prompt",
        lambda *args, **kwargs: "",
    )
    monkeypatch.setattr("nullion.system_context.build_system_context_snapshot", lambda tool_registry: object())
    monkeypatch.setattr("nullion.system_context.format_compact_system_context_for_prompt", lambda snapshot: "")
    monkeypatch.setattr("nullion.workspace_storage.format_workspace_storage_for_prompt", lambda principal_id: "")
    monkeypatch.setattr(
        "nullion.cron_execution_tools.build_cron_connector_scope_decision",
        lambda **kwargs: SimpleNamespace(allow_connector_tools=False, provider_ids=()),
    )
    monkeypatch.setattr("nullion.cron_execution_tools.structured_cron_passthrough_tool_names", lambda registry: ())

    tool_result = ToolResult(
        "quote-1",
        "stock_quote",
        "completed",
        {
            "items": [
                {"name": "Critical Metals Corp", "price": "$9.41"},
                {"name": "Microsoft Corp", "price": "$403.45"},
            ]
        },
    )
    fallback = sanitize_user_visible_reply(
        user_message="Generate the mid-market stock alert.",
        reply="The watchlist is mostly quiet.",
        tool_results=[tool_result],
    )
    assert fallback == "Results I found\n\n1. Critical Metals Corp\n   $9.41\n\n2. Microsoft Corp\n   $403.45"

    repaired_report = (
        "📈 Mid-market stock check\n\n"
        "Immediate attention:\n"
        "- CRML and MSFT are the only verified quote rows from this run.\n\n"
        "Watchlist pulse:\n"
        "- Need fresh quote evidence for the remaining symbols before ranking winners and laggards."
    )

    class FakeModelClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):  # noqa: ANN003
            self.calls.append(kwargs)
            return {"content": [{"type": "text", "text": json.dumps({"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"], "report": repaired_report})}]}

    model_client = FakeModelClient()

    class FakeOrchestrator:
        def __init__(self) -> None:
            self.model_client = model_client

        def run_turn(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            return SimpleNamespace(
                final_text=fallback,
                tool_results=[tool_result],
                artifacts=[],
                suspended_for_approval=False,
                approval_id=None,
                reached_iteration_limit=False,
                raw_tool_payload_blocked=False,
            )

    events: list[tuple[str, dict[str, object]]] = []

    result = run_single_agent_cron_turn(
        Job(
            name="Mid-Market Stock Alert",
            task="Generate a concise mid-market stock alert. No tables.",
        ),
        "telegram:100001",
        label="Scheduled task",
        orchestrator=FakeOrchestrator(),
        runtime=SimpleNamespace(store=None),
        tool_registry=ToolRegistry(),
        settings=SimpleNamespace(enabled_skill_packs=()),
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
    )

    assert result["cron_agent_final_repaired"] is True
    assert result["cron_agent_final_repair_reason"] == "structured_evidence_fallback"
    assert result["text"] == repaired_report
    assert model_client.calls
    delivered = cron_delivery_text_from_result(result)
    assert "Results I found" not in delivered
    assert "Mid-market stock check" in delivered
    assert any(kwargs.get("stage") == "final_repaired" for _event_type, kwargs in events)


def test_manual_cron_status_card_animates_and_finishes_with_check_mark() -> None:
    job = Job(id="ptown", name="Provincetown Airbnb Monitor")
    store = PlatformTaskCardStore(platform_activity_capabilities("telegram"))
    running_text = manual_cron_running_status_text(job, label="Manual scheduled task run")
    next_running_text = manual_cron_running_status_text(job, label="Manual scheduled task run", frame_index=1)

    first = store.update(
        target_id="100001",
        group_id="manual-cron-ptown",
        status_kind="task_summary",
        text=running_text,
    )
    second = store.update(
        target_id="100001",
        group_id="manual-cron-ptown",
        status_kind="task_summary",
        text=running_text,
    )
    terminal = store.update(
        target_id="100001",
        group_id="manual-cron-ptown",
        status_kind="task_summary",
        text=manual_cron_terminal_status_text(
            job,
            label="Manual scheduled task run",
            result={"cron_delivery_status": "sent"},
        ),
    )

    assert first is not None and "◑ Running: Manual scheduled task run started." in first
    assert second is not None and "◒ Running: Manual scheduled task run started." in second
    assert running_text != next_running_text
    assert "◐ Running: Manual scheduled task run started." in running_text
    assert "◓ Running: Manual scheduled task run started." in next_running_text
    assert terminal is not None
    assert "✓ Completed: Manual scheduled task run finished." in terminal
    assert "Running:" not in terminal


def test_manual_cron_background_delivery_pushes_running_and_terminal_status() -> None:
    job = Job(id="cron-status", name="Status cron")
    updates: list[tuple[str, str, bool]] = []
    terminal_seen = threading.Event()

    def _status_update(group_id: str, text: str, terminal: bool) -> None:
        updates.append((group_id, text, terminal))
        if terminal:
            terminal_seen.set()

    def _runner(cron_job, *, label, callbacks):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        time.sleep(0.035)
        return {"cron_delivery_status": "sent"}

    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0.01,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )

    assert terminal_seen.wait(1.0)
    assert updates
    assert all(group_id == receipt["task_group_id"] for group_id, _, _ in updates)
    running_updates = [text for _, text, terminal in updates if not terminal]
    assert any("Running: Manual scheduled task run started." in text for text in running_updates)
    assert len(set(running_updates)) >= 2
    assert updates[-1][2] is True
    assert "✓ Completed: Manual scheduled task run finished." in updates[-1][1]


def test_manual_cron_background_delivery_returns_receipt_before_workflow_starts() -> None:
    job = Job(id="cron-grace", name="Grace cron")
    runner_started = threading.Event()

    def _runner(cron_job, *, label, callbacks):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        runner_started.set()
        return {"cron_delivery_status": "sent"}

    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        workflow_runner=_runner,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0.2,
    )

    assert receipt["cron_delivery_status"] == "deferred"
    assert not runner_started.wait(0.05)
    assert runner_started.wait(1.0)


def test_manual_cron_background_delivery_uses_derived_agent_conversation_id() -> None:
    job = Job(id="cron-background-agent", name="Background agent cron")
    runner_seen = threading.Event()
    agent_conversation_ids: list[str] = []

    def _runner(cron_job, *, label, callbacks, agent_conversation_id=None):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        agent_conversation_ids.append(agent_conversation_id)
        runner_seen.set()
        return {"cron_delivery_status": "sent"}

    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "web",
            delivery_target=lambda cron_job, channel: "web:operator",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: True,
            send_platform_delivery=lambda cron_job, channel, target, text: False,
        ),
        origin_conversation_id="web:operator",
        workflow_runner=_runner,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
        task_group_id="manual-cron-test",
    )

    assert receipt["cron_delivery_status"] == "deferred"
    assert runner_seen.wait(1.0)
    assert agent_conversation_ids == ["web:operator:background:manual-cron-test"]


def test_manual_cron_background_delivery_marks_status_owned_when_initial_card_delivers() -> None:
    job = Job(id="cron-owned-status", name="Owned status cron")
    terminal_seen = threading.Event()

    def _status_update(group_id: str, text: str, terminal: bool) -> bool:
        _ = (group_id, text)
        if terminal:
            terminal_seen.set()
        return True

    def _runner(cron_job, *, label, callbacks):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        return {"cron_delivery_status": "sent"}

    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )

    assert receipt["status_delivered"] is True
    assert receipt["planner_status_owned_by_background"] is True
    assert receipt["foreground_reply_suppressed"] is True
    assert terminal_seen.wait(1.0)


def test_manual_cron_background_delivery_keeps_foreground_fallback_when_initial_card_fails() -> None:
    job = Job(id="cron-unowned-status", name="Unowned status cron")
    terminal_seen = threading.Event()

    def _status_update(group_id: str, text: str, terminal: bool) -> bool:
        _ = (group_id, text)
        if terminal:
            terminal_seen.set()
        return False

    def _runner(cron_job, *, label, callbacks):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        return {"cron_delivery_status": "sent"}

    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )

    assert receipt["status_delivered"] is False
    assert "planner_status_owned_by_background" not in receipt
    assert "foreground_reply_suppressed" not in receipt
    assert terminal_seen.wait(1.0)


def test_manual_cron_background_delivery_does_not_block_on_slow_initial_status() -> None:
    job = Job(id="cron-slow-status", name="Slow status cron")
    status_started = threading.Event()
    status_release = threading.Event()
    runner_started = threading.Event()

    def _status_update(group_id: str, text: str, terminal: bool) -> bool:
        _ = (group_id, text, terminal)
        status_started.set()
        status_release.wait(1.0)
        return True

    def _runner(cron_job, *, label, callbacks):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        runner_started.set()
        return {"cron_delivery_status": "sent"}

    started_at = time.perf_counter()
    receipt = start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.01,
    )
    elapsed = time.perf_counter() - started_at
    status_release.set()

    assert status_started.wait(0.5)
    assert elapsed < 0.2
    assert receipt["status_delivered"] is False
    assert "planner_status_owned_by_background" not in receipt
    assert runner_started.wait(1.0)


def test_manual_cron_background_delivery_can_be_stopped_by_conversation() -> None:
    job = Job(id="cron-stop", name="Stop cron")
    conversation_id = "telegram:stop-test"
    updates: list[tuple[str, str, bool]] = []
    terminal_seen = threading.Event()
    runner_started = threading.Event()

    def _status_update(group_id: str, text: str, terminal: bool) -> None:
        updates.append((group_id, text, terminal))
        if terminal:
            terminal_seen.set()

    def _runner(cron_job, *, label, callbacks, cancellation_checker=None):  # noqa: ANN001
        _ = (cron_job, label, callbacks)
        runner_started.set()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if cancellation_checker is not None and cancellation_checker():
                return {"cron_delivery_status": "cancelled", "cron_run_cancelled": True}
            time.sleep(0.01)
        return {"cron_delivery_status": "sent"}

    start_manual_cron_background_delivery(
        job,
        label="Manual scheduled task run",
        callbacks=CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        ),
        origin_conversation_id=conversation_id,
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0.01,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )

    assert runner_started.wait(1.0)
    assert cancel_manual_cron_background_runs(conversation_id) == 1
    assert terminal_seen.wait(1.0)
    terminal_updates = [text for _, text, terminal in updates if terminal]
    assert len(terminal_updates) == 1
    assert "✕ Stopped: Manual scheduled task run stopped." in terminal_updates[0]


def test_manual_cron_background_delivery_serializes_same_conversation() -> None:
    conversation_id = "telegram:bulk-cron"
    first_can_finish = threading.Event()
    second_started = threading.Event()
    terminal_seen = threading.Event()
    started: list[str] = []

    def _callbacks() -> CronRunDeliveryCallbacks:
        return CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        )

    def _runner(cron_job, *, label, callbacks, cancellation_checker=None):  # noqa: ANN001
        _ = (label, callbacks, cancellation_checker)
        started.append(cron_job.id)
        if cron_job.id == "first":
            assert first_can_finish.wait(1.0)
        else:
            second_started.set()
        return {"cron_delivery_status": "sent"}

    def _status_update(group_id: str, text: str, terminal: bool) -> None:
        _ = (group_id, text)
        if terminal:
            terminal_seen.set()

    start_manual_cron_background_delivery(
        Job(id="first", name="First cron"),
        label="Manual scheduled task run",
        callbacks=_callbacks(),
        origin_conversation_id=conversation_id,
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
    )
    start_manual_cron_background_delivery(
        Job(id="second", name="Second cron"),
        label="Manual scheduled task run",
        callbacks=_callbacks(),
        origin_conversation_id=conversation_id,
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0,
        background_start_grace_seconds=0,
    )

    time.sleep(0.05)
    assert started == ["first"]
    assert not second_started.is_set()
    first_can_finish.set()
    assert second_started.wait(1.0)
    assert terminal_seen.wait(1.0)
    assert started == ["first", "second"]


def test_manual_cron_background_delivery_does_not_animate_queued_same_conversation() -> None:
    conversation_id = "telegram:bulk-cron-status"
    first_started = threading.Event()
    first_can_finish = threading.Event()
    second_started = threading.Event()
    terminal_seen = threading.Event()
    updates: list[tuple[str, str, bool]] = []
    updates_lock = threading.Lock()

    def _callbacks() -> CronRunDeliveryCallbacks:
        return CronRunDeliveryCallbacks(
            effective_channel=lambda cron_job: "telegram",
            delivery_target=lambda cron_job, channel: "100001",
            run_agent_turn=lambda cron_job, conv_id: {"text": "done"},
            record_event=lambda *args, **kwargs: None,
            block_reason=lambda result, text, artifacts: None,
            save_web_delivery=lambda cron_job, conv_id, text, artifacts, result: False,
            send_platform_delivery=lambda cron_job, channel, target, text: True,
        )

    def _runner(cron_job, *, label, callbacks, cancellation_checker=None):  # noqa: ANN001
        _ = (label, callbacks, cancellation_checker)
        if cron_job.id == "queued-first":
            first_started.set()
            assert first_can_finish.wait(1.0)
        else:
            second_started.set()
        return {"cron_delivery_status": "sent"}

    def _status_update(group_id: str, text: str, terminal: bool) -> None:
        with updates_lock:
            updates.append((group_id, text, terminal))
        if terminal:
            terminal_seen.set()

    start_manual_cron_background_delivery(
        Job(id="queued-first", name="First cron"),
        label="Manual scheduled task run",
        callbacks=_callbacks(),
        origin_conversation_id=conversation_id,
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0.01,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )
    assert first_started.wait(1.0)
    second_receipt = start_manual_cron_background_delivery(
        Job(id="queued-second", name="Second cron"),
        label="Manual scheduled task run",
        callbacks=_callbacks(),
        origin_conversation_id=conversation_id,
        workflow_runner=_runner,
        status_update_callback=_status_update,
        status_update_interval_seconds=0.01,
        background_start_grace_seconds=0,
        initial_status_timeout_seconds=0.2,
    )

    time.sleep(0.05)
    with updates_lock:
        queued_updates = [
            update
            for update in updates
            if update[0] == second_receipt["task_group_id"] and not update[2]
        ]
    assert len(queued_updates) == 1
    assert not second_started.is_set()

    first_can_finish.set()
    assert second_started.wait(1.0)
    assert terminal_seen.wait(1.0)


def test_cron_run_delivery_workflow_cancellation_does_not_deliver_result() -> None:
    sent: list[str] = []
    events: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id, cancellation_checker=None: {"text": "Should not deliver"},
        record_event=lambda event, *args, **kwargs: events.append(event),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, target, text: sent.append(text) is None,
    )

    result = run_cron_delivery_workflow(
        Job(),
        label="Manual scheduled task run",
        callbacks=callbacks,
        cancellation_checker=lambda: True,
    )

    assert result["cron_delivery_status"] == "cancelled"
    assert result["cron_run_cancelled"] is True
    assert sent == []
    assert events == ["cron.delivery.started", "cron.delivery.cancelled"]


def test_cron_run_delivery_workflow_retries_messaging_delivery_once() -> None:
    attempts = {"count": 0}
    events: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": "Done"},
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: attempts.__setitem__("count", attempts["count"] + 1) or attempts["count"] == 2,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert attempts["count"] == 2
    assert "cron.delivery.retry" in events


def test_cron_run_delivery_workflow_does_not_retry_attachment_delivery_failure(tmp_path: Path) -> None:
    attempts = {"count": 0}
    events: list[str] = []
    artifact = tmp_path / "cron-output.pdf"
    artifact.write_bytes(b"%PDF-1.4\n%cron\n")

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"text": f"Done\nMEDIA:{artifact}"},
        record_event=lambda event_type, *args, **kwargs: events.append(event_type),
        block_reason=lambda result, text, artifacts: None,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: attempts.__setitem__("count", attempts["count"] + 1) or False,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert attempts["count"] == 2
    assert "cron.delivery.retry" not in events


def test_cron_run_delivery_workflow_blocks_unfinished_results_before_delivery() -> None:
    events: list[tuple[str, dict]] = []
    cleared: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {"reached_iteration_limit": True, "text": "stopped"},
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
        block_reason=lambda result, text, artifacts: "cron_run_reached_iteration_limit",
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: (_ for _ in ()).throw(AssertionError("should not send")),
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert result["cron_delivery_failed"] is True
    assert result["cron_run_failed"] is True
    assert result["reason"] == "cron_run_reached_iteration_limit"
    assert cleared == ["telegram:42"]
    assert events[-1] == ("cron.delivery.failed", {"reason": "cron_run_reached_iteration_limit"})


def test_cron_run_delivery_workflow_sends_failure_status_without_unfinished_findings() -> None:
    events: list[tuple[str, dict]] = []
    sent: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {
            "text": "I couldn't fully complete this yet. The task is still open.",
            "model_timed_out": True,
            "response_fulfilled": False,
            "tool_results": [
                ToolResult("quote-1", "market_quote", "completed", {"symbols": [{"symbol": "MSFT"}]})
            ],
        },
        record_event=lambda event_type, *args, **kwargs: events.append((event_type, dict(kwargs))),
        block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(
        Job(delivery_channel="telegram"),
        label="Scheduled task",
        callbacks=callbacks,
    )

    assert len(sent) == 1 and "timed out" in sent[0]
    assert "MSFT" not in sent[0]
    assert result["cron_delivery_status"] == "sent"
    assert result["reason"] == "cron_report_incomplete"
    assert result["execution_outcome"]["execution_succeeded"] is False


def test_cron_final_repair_can_recover_typed_model_timeout_from_completed_evidence() -> None:
    class RepairClient:
        def create(self, **kwargs):  # noqa: ANN003
            assert "Verified compact tool evidence" in kwargs["messages"][0]["content"][0]["text"]
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"], "report": "MSFT closed at $510.20, up 1.2%; no other requested symbol was verified."}),
                    }
                ],
            }

    result = {
        "text": "I couldn't fully complete this yet. The task is still open.",
        "model_timed_out": True,
        "response_fulfilled": False,
        "artifact_delivery_required": False,
        "artifact_delivery_satisfied": True,
        "tool_results": [
            ToolResult(
                "quote-1",
                "market_quote",
                "completed",
                {"symbols": [{"symbol": "MSFT", "price": 510.2, "change_percent": 1.2}]},
            )
        ],
    }

    _repair_cron_agent_final_text_if_needed(
        Job(name="Market brief", task="Report verified closing prices."),
        result,
        model_client=RepairClient(),
    )

    assert result["cron_agent_final_repaired"] is True
    assert result["model_timed_out"] is False
    assert result["response_fulfilled"] is True
    assert result["text"].startswith("MSFT closed")
    assert cron_delivery_block_reason(result, result["text"], None) is None


def test_cron_final_repair_does_not_replace_a_missing_required_artifact_with_text() -> None:
    class RepairClient:
        def create(self, **kwargs):  # noqa: ANN003
            raise AssertionError("a text-only repair cannot satisfy a typed artifact contract")

    result = {
        "text": "I couldn't fully complete this yet. The task is still open.",
        "model_timed_out": True,
        "response_fulfilled": False,
        "artifact_delivery_required": True,
        "artifact_delivery_satisfied": False,
        "required_artifact_extensions": [".html"],
        "tool_results": [
            ToolResult(
                "quote-1",
                "market_quote",
                "completed",
                {"symbols": [{"symbol": "MSFT", "price": 510.2}]},
            )
        ],
    }

    _repair_cron_agent_final_text_if_needed(
        Job(name="Market report", task="Produce the scoped artifact."),
        result,
        model_client=RepairClient(),
    )

    assert "cron_agent_final_repaired" not in result
    assert result["model_timed_out"] is True
    assert cron_delivery_block_reason(result, result["text"], None) == "cron_run_model_timeout"


def test_cron_run_delivery_workflow_keeps_email_attachment_text_out_of_telegram(tmp_path: Path) -> None:
    ocr_sidecar = tmp_path / "uscis_notice_ocr.txt"
    ocr_sidecar.write_text(
        "NOTICE DATE\n07/03/2026\nAPPLICATION/PETITION/REQUEST NUMBER\nIOE0000000000",
        encoding="utf-8",
    )
    sent: list[str] = []

    callbacks = CronRunDeliveryCallbacks(
        effective_channel=lambda job: "telegram",
        delivery_target=lambda job, channel: "42",
        run_agent_turn=lambda job, conv_id: {
            "text": "One dated notice was reviewed; no calendar event was created because the due date was unclear.",
            "artifacts": [{"path": str(ocr_sidecar)}],
            "tool_results": [
                ToolResult("read-1", "email_attachment_read", "completed", {"path": str(ocr_sidecar)})
            ],
        },
        record_event=lambda event_type, *args, **kwargs: None,
        block_reason=cron_delivery_block_reason,
        save_web_delivery=lambda job, conv_id, text, artifacts, result: False,
        send_platform_delivery=lambda job, channel, text: sent.append(text) or True,
        start_background_delivery=lambda conv_id, job: None,
        clear_background_delivery=lambda conv_id: None,
    )

    result = run_cron_delivery_workflow(
        Job(name="Account triage", delivery_channel="telegram"),
        label="Scheduled task",
        callbacks=callbacks,
    )

    assert result["cron_delivery_status"] == "sent"
    assert len(sent) == 1
    assert "One dated notice was reviewed" in sent[0]
    assert "APPLICATION/PETITION/REQUEST NUMBER" not in sent[0]
    assert "IOE0000000000" not in sent[0]


def test_cron_delivery_blocks_source_email_pdf_media_and_ocr_preview(tmp_path: Path) -> None:
    source_pdf = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "appointment.pdf"
    source_pdf.parent.mkdir(parents=True)
    source_pdf.write_bytes(b"%PDF-1.4\n")
    preview = (
        "NOTICE DATE\n07/03/2026\nAPPLICATION/PETITION/REQUEST NUMBER\n"
        "IOE0000000000\nPAGE\n1 of 2\n"
        "APPLICANT NAME\nEXAMPLE PERSON\nSERVICE CENTER\nEXAMPLE CENTER\n"
        "APPOINTMENT INFORMATION\nBring this notice and identification to the appointment.\n"
        "MAILING ADDRESS\n40 EXAMPLE STREET FLOOR 17\nNEW YORK NY 10000"
    )
    result_payload = {
        "text": (
            "I found the attachment.\n\n"
            f"Preview: {preview}\n\n"
            f"MEDIA:{source_pdf}"
        ),
        "tool_results": [
            ToolResult(
                "attachment",
                "email_attachment_read",
                "completed",
                {
                    "filename": source_pdf.name,
                    "mime_type": "application/pdf",
                    "size_bytes": source_pdf.stat().st_size,
                    "artifact_path": str(source_pdf),
                    "artifact_paths": [str(source_pdf)],
                    "text_preview": preview,
                    "artifact_descriptors": [
                        {"path": str(source_pdf), "role": "source", "kind": "email_attachment"}
                    ],
                },
            )
        ],
    }

    assert cron_delivery_artifact_paths_from_result(result_payload, result_payload["text"]) == ()
    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:42")
    assert "MEDIA:" not in delivered
    assert str(source_pdf) not in delivered
    assert "NOTICE DATE" not in delivered
    assert "APPLICATION/PETITION/REQUEST NUMBER" not in delivered
    assert result_payload["cron_sensitive_tool_body_withheld"] is True


def test_cron_delivery_keeps_explicit_deliverable_artifact_descriptor(tmp_path: Path) -> None:
    report = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "requested-report.pdf"
    report.parent.mkdir(parents=True)
    report.write_bytes(b"%PDF-1.4\n")
    result_payload = {
        "text": "Requested report ready.",
        "tool_results": [
            ToolResult(
                "producer",
                "email_attachment_read",
                "completed",
                {
                    "artifact_path": str(report),
                    "artifact_descriptors": [
                        {"path": str(report), "role": "deliverable", "kind": "email_attachment"}
                    ],
                },
            )
        ],
    }

    delivered = cron_delivery_text_from_result(result_payload, principal_id="telegram:42")
    assert delivered == f"Requested report ready.\n\nMEDIA:{report}"
    assert cron_delivery_artifact_paths_from_result(result_payload, delivered) == (str(report),)


def test_cron_raw_browser_footer_copy_requires_final_repair() -> None:
    raw_text = "Upcoming Dates\n© 2026, Example Events Powered by Site Builder"
    result_payload = {
        "text": (
            "Here are the most relevant details I could verify from the source:\n\n"
            "- Upcoming Dates\n"
            "- © 2026, Example Events Powered by Site Builder"
        ),
        "tool_results": [
            ToolResult("extract", "browser_extract_text", "completed", {"text": raw_text})
        ],
    }

    assert cron_structured_result_block_reason(
        result_payload,
        None,
        text=result_payload["text"],
    ) == "cron_run_internal_tool_output_leaked"


def test_cron_delivery_filters_scratch_media_outside_required_artifact_extensions() -> None:
    job = Job(
        artifact_delivery_options={
            "requires_attachment_delivery": True,
            "required_artifact_extensions": [".html"],
            "deliver_required_extensions_only": True,
        }
    )

    filtered = _filter_cron_delivery_media_to_required_extensions(
        job,
        "Report ready.\nMEDIA:/tmp/report.html\nWorking notes\nMEDIA:/tmp/notes.txt",
    )

    assert filtered == "Report ready.\nMEDIA:/tmp/report.html\nWorking notes"


def assert_channel_without_media(channel: str, text: str, artifact: str) -> bool:
    assert channel == "telegram"
    assert "Done" in text
    assert f"MEDIA:{artifact}" not in text
    assert artifact not in text
    assert "MEDIA:" not in text
    return True
