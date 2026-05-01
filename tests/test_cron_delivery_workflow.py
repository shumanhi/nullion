from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nullion.cron_delivery import (
    CronRunDeliveryCallbacks,
    configured_delivery_target,
    cron_conversation_id,
    cron_delivery_target,
    cron_delivery_text,
    effective_cron_delivery_channel,
    normalize_cron_delivery_channel,
    run_cron_delivery_workflow,
)


@dataclass
class Job:
    id: str = "cron-1"
    name: str = "Morning brief"
    task: str = "make the brief"
    delivery_channel: str = ""
    delivery_target: str = ""


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


def test_cron_delivery_text_appends_deduped_media_directives() -> None:
    artifact = Path("/tmp/nullion-artifact-report.html")

    assert cron_delivery_text("Done", [{"path": str(artifact)}, {"path": str(artifact)}]) == (
        "Done\n\nMEDIA:/tmp/nullion-artifact-report.html"
    )


def test_unknown_cron_channel_normalizes_to_blank() -> None:
    assert normalize_cron_delivery_channel("sms") == ""


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
    assert saved == [("web:operator", "Done")]
    assert events == ["cron.delivery.started", "cron.delivery.saved"]


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
            assert_channel_and_media(channel, text, str(artifact)) and False
        ),
        start_background_delivery=lambda conv_id, job: started.append(conv_id),
        clear_background_delivery=lambda conv_id: cleared.append(conv_id),
    )

    result = run_cron_delivery_workflow(Job(delivery_channel="telegram"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "failed"
    assert result["cron_delivery_failed"] is True
    assert started == ["telegram:42"]
    assert cleared == []
    assert events[-1] == ("cron.delivery.failed", {"reason": "missing bot token or target"})


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


def assert_channel_and_media(channel: str, text: str, artifact: str) -> bool:
    assert channel == "telegram"
    assert "Done" in text
    assert f"MEDIA:{artifact}" in text
    return True
