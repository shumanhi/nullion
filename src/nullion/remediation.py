"""Remediation playbooks — maps service/issue codes to recovery actions.

Each playbook maps a service_id to:
  - A human-readable summary shown in the doctor card
  - Specific button labels and their action keys
  - An optional auto_heal_fn that fires before surfacing a card to the user

The auto-heal function takes (runtime, ProbeResult) and returns True if it
successfully healed the service (no card shown), False if it failed or was
unable to heal (card shown).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from nullion.health import HealthIssueType

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RemediationPlaybook:
    service_id: str
    recommendation_code: str
    summary: str                          # Shown in doctor card header
    issue_type: HealthIssueType
    button_labels: list[str]              # Human-readable, e.g. ["Retry now", "Switch model"]
    button_commands: list[str]            # Corresponding action keys
    auto_heal_fn: Callable | None = None  # Called before escalating; True = healed


_PLAYBOOKS: dict[str, RemediationPlaybook] = {}


def register_playbook(playbook: RemediationPlaybook) -> None:
    _PLAYBOOKS[playbook.service_id] = playbook


def playbook_for_service(service_id: str) -> RemediationPlaybook | None:
    """Exact match first, then prefix match for plugin namespaced IDs."""
    if service_id in _PLAYBOOKS:
        return _PLAYBOOKS[service_id]
    if service_id.startswith("plugin:"):
        return _PLAYBOOKS.get("plugin:*")
    return None


def playbook_for_recommendation_code(recommendation_code: str | None) -> RemediationPlaybook | None:
    if not recommendation_code:
        return None
    for playbook in _PLAYBOOKS.values():
        if playbook.recommendation_code == recommendation_code:
            return playbook
    return None


def remediation_buttons_for_recommendation_code(recommendation_code: str | None) -> tuple[tuple[str, str], ...]:
    playbook = playbook_for_recommendation_code(recommendation_code)
    if playbook is None:
        return ()
    return tuple(zip(playbook.button_labels, playbook.button_commands))


# ── Built-in auto-heal helpers ────────────────────────────────────────────────

def _auto_heal_restart_bot(runtime, result) -> bool:
    """Trigger graceful service rebuild via SIGHUP — handled by telegram_app.main()."""
    import os
    import signal as _signal
    try:
        os.kill(os.getpid(), _signal.SIGHUP)
        logger.info("Remediation: sent SIGHUP to self for graceful service restart")
        return True  # Optimistic — monitor confirms on next probe cycle
    except Exception as exc:
        logger.warning("Remediation: failed to send SIGHUP: %s", exc)
        return False


# ── Built-in playbooks ─────────────────────────────────────────────────────────

register_playbook(RemediationPlaybook(
    service_id="model_api",
    recommendation_code="model_api_unreachable",
    summary="The AI model API isn't responding",
    issue_type=HealthIssueType.TIMEOUT,
    button_labels=["Retry now", "Switch to fallback model", "Pause chat"],
    button_commands=["doctor:retry_model_api", "doctor:switch_fallback_model", "doctor:pause_chat"],
    auto_heal_fn=None,   # Can't auto-heal a down external API — surface the card
))

register_playbook(RemediationPlaybook(
    service_id="telegram_bot",
    recommendation_code="telegram_bot_unreachable",
    summary="The Telegram bot connection is degraded",
    issue_type=HealthIssueType.DEGRADED,
    button_labels=["Reconnect", "Restart bot"],
    button_commands=["doctor:reconnect_telegram", "doctor:restart_bot"],
    auto_heal_fn=_auto_heal_restart_bot,
))

register_playbook(RemediationPlaybook(
    service_id="slack_bot",
    recommendation_code="slack_bot_unreachable",
    summary="The Slack adapter connection is degraded",
    issue_type=HealthIssueType.DEGRADED,
    button_labels=["Reconnect Slack", "Restart Slack adapter"],
    button_commands=["doctor:reconnect_slack", "doctor:restart_slack_adapter"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="discord_bot",
    recommendation_code="discord_bot_unreachable",
    summary="The Discord adapter connection is degraded",
    issue_type=HealthIssueType.DEGRADED,
    button_labels=["Reconnect Discord", "Restart Discord adapter"],
    button_commands=["doctor:reconnect_discord", "doctor:restart_discord_adapter"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="plugin:*",
    recommendation_code="plugin_unreachable",
    summary="A connected plugin stopped responding",
    issue_type=HealthIssueType.ERROR,
    button_labels=["Restart plugin", "Disable plugin"],
    button_commands=["doctor:restart_plugin", "doctor:disable_plugin"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="workflow_timeout",
    recommendation_code="investigate_timeout",
    summary="A workflow timed out",
    issue_type=HealthIssueType.TIMEOUT,
    button_labels=["Inspect run", "Cancel run", "Retry workflow"],
    button_commands=["doctor:inspect_run", "doctor:cancel_run", "doctor:retry_workflow"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="workflow_stalled",
    recommendation_code="investigate_stall",
    summary="A workflow stopped reporting progress",
    issue_type=HealthIssueType.STALLED,
    button_labels=["Inspect run", "Cancel run", "Retry workflow"],
    button_commands=["doctor:inspect_run", "doctor:cancel_run", "doctor:retry_workflow"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="missing_capsule_reference",
    recommendation_code="repair_missing_capsule_reference",
    summary="A scheduled task references a missing capsule",
    issue_type=HealthIssueType.ERROR,
    button_labels=["Open schedule", "Disable task"],
    button_commands=["doctor:open_schedule", "doctor:disable_task"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="approval_backlog",
    recommendation_code="approval_backlog",
    summary="Approvals are waiting for operator review",
    issue_type=HealthIssueType.ISSUE,
    button_labels=["Review approvals", "Clear stale approvals"],
    button_commands=["doctor:review_approvals", "doctor:clear_stale_approvals"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="runtime_storage",
    recommendation_code="runtime_storage_issue",
    summary="Runtime storage needs attention",
    issue_type=HealthIssueType.ERROR,
    button_labels=["Run diagnosis", "Create backup", "Repair checkpoint"],
    button_commands=["doctor:run_diagnosis", "doctor:create_backup", "doctor:repair_checkpoint"],
    auto_heal_fn=None,
))

register_playbook(RemediationPlaybook(
    service_id="model_quota",
    recommendation_code="model_quota_exhausted",
    summary="The configured model provider quota is exhausted",
    issue_type=HealthIssueType.ERROR,
    button_labels=["Switch model", "Retry later", "Pause chat"],
    button_commands=["doctor:switch_fallback_model", "doctor:retry_later", "doctor:pause_chat"],
    auto_heal_fn=None,
))


__all__ = [
    "RemediationPlaybook",
    "playbook_for_recommendation_code",
    "playbook_for_service",
    "register_playbook",
    "remediation_buttons_for_recommendation_code",
]
