from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path


LEGACY_CAPSULE_ID = "cap-compat"
LEGACY_APPROVAL_ID = "approval-compat"
LEGACY_GRANT_ID = "grant-compat"
LEGACY_PERMIT_ID = "permit-compat"
LEGACY_CONVERSATION_ID = "conv-compat"
LEGACY_BRANCH_ID = "branch-compat"
LEGACY_TURN_ID = "turn-compat"
LEGACY_FRAME_ID = "frame-compat"
LEGACY_MISSION_ID = "mission-compat"

_NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC).isoformat()

_COLLECTION_TABLES = {
    "capsules": "runtime_capsules",
    "scheduled_tasks": "reminders_crons",
    "reminders": "reminders_crons",
    "suspended_turns": "runtime_suspended_turns",
    "doctor_signals": "doctor_actions",
    "approval_requests": "approvals",
    "permission_grants": "permission_grants",
    "boundary_permits": "boundary_rules",
    "boundary_policy_rules": "boundary_rules",
    "mini_agent_runs": "mini_agent_runs",
    "missions": "tasks_missions",
    "task_frames": "tasks_missions",
    "active_task_frames": "tasks_missions",
    "builder_proposals": "builder_proposals",
    "skills": "runtime_skills",
    "skill_execution_plans": "runtime_skills",
    "user_facts": "memory",
    "preferences": "memory",
    "environment_facts": "memory",
    "conversation_turns": "conversation_events",
    "conversation_branches": "conversation_events",
    "conversation_heads": "conversation_events",
    "conversation_events": "conversation_events",
}

_TABLES = tuple(dict.fromkeys(_COLLECTION_TABLES.values()))


def _legacy_runtime_rows() -> list[tuple[str, str, dict[str, object]]]:
    return [
        (
            "capsules",
            LEGACY_CAPSULE_ID,
            {
                "capsule_id": LEGACY_CAPSULE_ID,
                "owner": "operator",
                "goal": "finish update compatibility smoke",
                "state": "pending",
                "risk_level": "normal",
                "active_mini_agents": [],
                "pending_approval_id": None,
                "success_criteria": ["app starts"],
            },
        ),
        (
            "approval_requests",
            LEGACY_APPROVAL_ID,
            {
                "approval_id": LEGACY_APPROVAL_ID,
                "requested_by": "web:operator",
                "action": "allow_boundary",
                "resource": "https://api.example.test/*",
                "request_kind": "boundary_policy",
                "context": {"boundary_kind": "outbound_network", "target": "https://api.example.test/*"},
                "created_at": _NOW,
                "status": "pending",
                "decided_at": None,
                "decided_by": None,
                "decision_reason": None,
            },
        ),
        (
            "permission_grants",
            LEGACY_GRANT_ID,
            {
                "grant_id": LEGACY_GRANT_ID,
                "approval_id": LEGACY_APPROVAL_ID,
                "principal_id": "global:operator",
                "permission": "tool:web_fetch",
                "granted_by": "operator",
                "granted_at": _NOW,
                "expires_at": None,
                "revoked_at": None,
                "revoked_by": None,
                "revoked_reason": None,
            },
        ),
        (
            "boundary_permits",
            LEGACY_PERMIT_ID,
            {
                "permit_id": LEGACY_PERMIT_ID,
                "approval_id": LEGACY_APPROVAL_ID,
                "principal_id": "global:operator",
                "boundary_kind": "outbound_network",
                "selector": "api.example.test",
                "granted_by": "operator",
                "granted_at": _NOW,
                "expires_at": _NOW,
                "uses_remaining": 3,
                "revoked_at": None,
                "revoked_by": None,
                "revoked_reason": None,
            },
        ),
        (
            "boundary_policy_rules",
            "rule-compat",
            {
                "rule_id": "rule-compat",
                "principal_id": "global:operator",
                "kind": "outbound_network",
                "mode": "allow",
                "selector": "api.example.test",
                "created_by": "operator",
                "created_at": _NOW,
                "expires_at": None,
                "priority": 0,
                "reason": "compat fixture",
                "revoked_at": None,
            },
        ),
        (
            "scheduled_tasks",
            "sched-compat",
            {
                "task_id": "sched-compat",
                "capsule_id": LEGACY_CAPSULE_ID,
                "schedule_kind": "recurring",
                "interval_minutes": 300,
                "enabled": True,
                "last_run_at": _NOW,
                "failure_count": 2,
            },
        ),
        (
            "reminders",
            "rem-compat",
            {
                "task_id": "rem-compat",
                "chat_id": "web:operator",
                "text": "review migration",
                "due_at": _NOW,
                "delivered_at": None,
            },
        ),
        (
            "missions",
            LEGACY_MISSION_ID,
            {
                "mission_id": LEGACY_MISSION_ID,
                "owner": "operator",
                "title": "Release compatibility",
                "goal": "Keep data intact",
                "status": "running",
                "continuation_policy": "auto_finish",
                "created_from_capsule_id": LEGACY_CAPSULE_ID,
                "active_capsule_id": LEGACY_CAPSULE_ID,
                "active_step_id": "step-compat",
                "steps": [
                    {
                        "step_id": "step-compat",
                        "title": "Validate DB",
                        "status": "running",
                        "kind": "Run migration",
                        "capsule_id": LEGACY_CAPSULE_ID,
                        "metadata": {},
                        "notes": None,
                        "mini_agent_run_id": None,
                        "mini_agent_run_ids": [],
                        "required_mini_agent_run_ids": [],
                    }
                ],
                "completion_checklist": [
                    {"item_id": "check-compat", "label": "Data survives", "required": True, "satisfied": False, "details": None}
                ],
                "created_at": _NOW,
                "updated_at": _NOW,
                "blocked_reason": None,
                "waiting_on": None,
                "last_progress_message": None,
                "result_summary": None,
                "terminal_reason": None,
            },
        ),
        (
            "task_frames",
            LEGACY_FRAME_ID,
            {
                "frame_id": LEGACY_FRAME_ID,
                "conversation_id": LEGACY_CONVERSATION_ID,
                "branch_id": LEGACY_BRANCH_ID,
                "source_turn_id": LEGACY_TURN_ID,
                "parent_frame_id": None,
                "status": "active",
                "operation": "generate_artifact",
                "target": {"kind": "file", "value": "compat report", "normalized_value": "compat report"},
                "execution": {
                    "preferred_tool_family": "workspace",
                    "fallback_tool_family": "fallback",
                    "approval_sensitive": True,
                    "boundary_kind": "filesystem",
                },
                "output": {"artifact_kind": "txt", "delivery_mode": "attachment", "response_shape": "text"},
                "finish": {
                    "requires_artifact_delivery": True,
                    "requires_attempt": True,
                    "required_artifact_kind": "txt",
                    "required_tool_completion": ["file_write"],
                },
                "summary": "compat summary",
                "created_at": _NOW,
                "updated_at": _NOW,
                "last_activity_turn_id": None,
                "completion_turn_id": None,
                "metadata": {"fixture": "legacy-sqlite"},
            },
        ),
        (
            "active_task_frames",
            LEGACY_CONVERSATION_ID,
            {"conversation_id": LEGACY_CONVERSATION_ID, "frame_id": LEGACY_FRAME_ID},
        ),
        (
            "builder_proposals",
            "proposal-compat",
            {
                "proposal_id": "proposal-compat",
                "proposal": {
                    "decision_type": "skill_proposal",
                    "title": "Compat Skill",
                    "summary": "Checks update DB compatibility",
                    "confidence": 0.8,
                    "approval_mode": "manual",
                    "suggested_skill_title": "Compat Skill",
                    "suggested_trigger": "when updating",
                    "suggested_steps": ["check db"],
                    "suggested_tags": ["update"],
                },
                "status": "pending",
                "created_at": _NOW,
                "resolved_at": None,
                "accepted_skill_id": None,
                "context_key": "compat",
            },
        ),
        (
            "mini_agent_runs",
            "mini-compat",
            {
                "run_id": "mini-compat",
                "capsule_id": LEGACY_CAPSULE_ID,
                "mini_agent_type": "researcher",
                "status": "running",
                "created_at": _NOW,
                "result_summary": "working",
            },
        ),
        (
            "skills",
            "skill-compat",
            {
                "skill_id": "skill-compat",
                "title": "Compat Skill",
                "summary": "Checks db",
                "trigger": "structured trigger",
                "steps": ["inspect db"],
                "tags": ["update"],
                "created_at": _NOW,
                "updated_at": _NOW,
                "revision": 1,
                "revision_history": [],
                "workflow_signals": [],
            },
        ),
        (
            "skill_execution_plans",
            LEGACY_CAPSULE_ID,
            {
                "capsule_id": LEGACY_CAPSULE_ID,
                "plan_id": "plan-compat",
                "skill_id": "skill-compat",
                "title": "Compat Skill",
                "steps": ["inspect db"],
                "step_states": ["pending"],
                "active_step_index": 0,
                "safety_mode": "suggest_only",
            },
        ),
        (
            "user_facts",
            "mem-fact",
            {
                "entry_id": "mem-fact",
                "owner": "operator",
                "kind": "fact",
                "key": "release_channel",
                "value": "beta",
                "source": "compat-fixture",
                "created_at": _NOW,
                "updated_at": _NOW,
                "last_used_at": None,
                "use_count": 0,
                "use_score": 0.0,
            },
        ),
        (
            "preferences",
            "mem-pref",
            {
                "entry_id": "mem-pref",
                "owner": "operator",
                "kind": "preference",
                "key": "theme",
                "value": "dark",
                "source": "compat-fixture",
                "created_at": _NOW,
                "updated_at": _NOW,
                "last_used_at": None,
                "use_count": 0,
                "use_score": 0.0,
            },
        ),
        (
            "environment_facts",
            "mem-env",
            {
                "entry_id": "mem-env",
                "owner": "operator",
                "kind": "environment_fact",
                "key": "os",
                "value": "linux",
                "source": "compat-fixture",
                "created_at": _NOW,
                "updated_at": _NOW,
                "last_used_at": None,
                "use_count": 0,
                "use_score": 0.0,
            },
        ),
        (
            "suspended_turns",
            "approval-suspended",
            {
                "approval_id": "approval-suspended",
                "conversation_id": LEGACY_CONVERSATION_ID,
                "chat_id": "web:operator",
                "message": "pending approval",
                "request_id": "req-compat",
                "message_id": "msg-compat",
                "created_at": _NOW,
                "mission_id": None,
                "pending_step_idx": None,
                "messages_snapshot": [{"role": "user", "content": "pending"}],
                "pending_tool_calls": [{"id": "tool-compat"}],
                "task_id": "task-compat",
                "group_id": "group-compat",
                "agent_id": None,
                "resume_token": None,
            },
        ),
        (
            "doctor_signals",
            "doctor-signal-compat",
            {"target": "doctor", "reason": "doctor", "severity": "medium", "summary": "legacy fixture boot", "error": None, "recommendation_code": None},
        ),
        (
            "conversation_turns",
            LEGACY_TURN_ID,
            {
                "turn_id": LEGACY_TURN_ID,
                "conversation_id": LEGACY_CONVERSATION_ID,
                "branch_id": LEGACY_BRANCH_ID,
                "parent_turn_id": None,
                "disposition": "independent",
                "user_message": "run update compatibility test",
                "status": "open",
                "created_at": _NOW,
                "disposition_reason": None,
                "started_snapshot_summary": None,
                "cancellation_token": None,
            },
        ),
        (
            "conversation_branches",
            LEGACY_BRANCH_ID,
            {
                "branch_id": LEGACY_BRANCH_ID,
                "conversation_id": LEGACY_CONVERSATION_ID,
                "status": "active",
                "created_from_turn_id": LEGACY_TURN_ID,
                "superseded_by_branch_id": None,
                "cancelled_at": None,
            },
        ),
        (
            "conversation_heads",
            LEGACY_CONVERSATION_ID,
            {"conversation_id": LEGACY_CONVERSATION_ID, "active_branch_id": LEGACY_BRANCH_ID, "active_turn_id": LEGACY_TURN_ID},
        ),
        (
            "conversation_events",
            "conversation-event-compat",
            {
                "conversation_id": LEGACY_CONVERSATION_ID,
                "event_type": "conversation.chat_turn",
                "created_at": _NOW,
                "payload": {"source": "compat-fixture"},
            },
        ),
    ]


def create_legacy_runtime_sqlite_db(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        for table in _TABLES:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    collection TEXT NOT NULL,
                    item_key   TEXT NOT NULL,
                    payload    TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (collection, item_key)
                )
                """
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_collection ON {table} (collection)")
        for collection, item_key, payload in _legacy_runtime_rows():
            conn.execute(
                f"""
                INSERT INTO {_COLLECTION_TABLES[collection]}
                    (collection, item_key, payload, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (collection, item_key, json.dumps(payload, sort_keys=True), _NOW),
            )
    return path
