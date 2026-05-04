"""JSON persistence helpers for RuntimeStore."""

from __future__ import annotations

from contextlib import contextmanager
import json
import logging
import os
import shutil
import sqlite3
import threading
from time import perf_counter
from datetime import UTC, datetime
from pathlib import Path

from cryptography.fernet import Fernet

from nullion.approvals import ApprovalRequest, ApprovalStatus, BoundaryPermit, PermissionGrant
from nullion.audit import AuditRecord
from nullion.builder import BuilderDecisionType, BuilderProposal, BuilderProposalRecord
from nullion.conversation_runtime import ConversationBranchStatus, ConversationTurnDisposition
from nullion.events import Event
from nullion.intent import IntentCapsule, IntentState
from nullion.mini_agent_runs import MiniAgentRun, MiniAgentRunStatus
from nullion.memory import UserMemoryEntry, UserMemoryKind
from nullion.missions import (
    MissionChecklistItem,
    MissionContinuationPolicy,
    MissionRecord,
    MissionStatus,
    MissionStep,
    MissionTerminalReason,
)
from nullion.policy import BoundaryKind, BoundaryPolicyRule, PolicyMode
from nullion.progress import ProgressState, ProgressUpdate
from nullion.reminders import ReminderRecord
from nullion.suspended_turns import SuspendedTurn
from nullion.runtime_store import ConversationBranch, ConversationTurn, RuntimeStore
from nullion.scheduler import ScheduleKind, ScheduledTask
from nullion.secure_storage import load_or_create_fernet_key
from nullion.sentinel_escalations import EscalationStatus, SentinelEscalationArtifact
from nullion.signals import SignalRoute, SignalTarget
from nullion.skill_planner import SkillExecutionPlan
from nullion.skills import SkillRecord, SkillRevision, SkillWorkflowSignal
from nullion.task_frames import (
    TaskFrame,
    TaskFrameExecutionContract,
    TaskFrameFinishCriteria,
    TaskFrameOperation,
    TaskFrameOutputContract,
    TaskFrameStatus,
    TaskFrameTarget,
)


RUNTIME_STORE_FORMAT_VERSION = 1
SUPPORTED_RUNTIME_STORE_FORMAT_VERSIONS = frozenset({RUNTIME_STORE_FORMAT_VERSION})
RUNTIME_STORE_BACKUP_DEPTH = 3
RUNTIME_SQLITE_MEASURE_ENABLED = os.environ.get("NULLION_SQLITE_MEASURE", "").lower() in {"1", "true", "yes"}
RUNTIME_SQLITE_SLOW_MS = float(os.environ.get("NULLION_SQLITE_SLOW_MS", "250"))
DEFAULT_NULLION_HOME = Path.home() / ".nullion"
DEFAULT_MEMORY_KEY_PATH = DEFAULT_NULLION_HOME / "memory.key"
MEMORY_KEYCHAIN_SERVICE = "Nullion Runtime Memory Key"
MEMORY_KEYCHAIN_ACCOUNT = "runtime_memory"
_ENCRYPTED_RUNTIME_MEMORY_PREFIX = "fernet:v1:"
_MEMORY_MIGRATION_FAILURES_TABLE = "memory_migration_failures"

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised on POSIX platforms
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None


@contextmanager
def _runtime_store_file_lock(target: Path):
    lock_path = target.with_name(f".{target.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
RUNTIME_SQLITE_SUFFIXES = frozenset({".db", ".sqlite", ".sqlite3"})

_BUILDER_PROPOSAL_FALLBACK_CREATED_AT = datetime.fromtimestamp(0, tz=UTC)


def _record_identity(record: object) -> str:
    for attr in (
        "event_id",
        "audit_id",
        "update_id",
        "run_id",
        "action_id",
        "escalation_id",
        "frame_id",
        "turn_id",
        "branch_id",
        "proposal_id",
        "skill_id",
        "task_id",
        "id",
    ):
        value = getattr(record, attr, None)
        if isinstance(value, str) and value:
            return f"{attr}:{value}"
    if isinstance(record, dict):
        for key in (
            "event_id",
            "audit_id",
            "update_id",
            "run_id",
            "action_id",
            "escalation_id",
            "frame_id",
            "turn_id",
            "branch_id",
            "proposal_id",
            "skill_id",
            "task_id",
            "id",
        ):
            value = record.get(key)
            if isinstance(value, str) and value:
                return f"{key}:{value}"
    return repr(record)


def _merge_list_records(current: list, previous: list) -> None:
    seen = {_record_identity(item) for item in current}
    for item in previous:
        identity = _record_identity(item)
        if identity in seen:
            continue
        current.append(item)
        seen.add(identity)


def _approval_merge_value(current: ApprovalRequest, previous: ApprovalRequest) -> ApprovalRequest:
    if current.status is ApprovalStatus.PENDING and previous.status is not ApprovalStatus.PENDING:
        return previous
    if current.decided_at is None and previous.decided_at is not None:
        return previous
    if current.decided_at is not None and previous.decided_at is not None and previous.decided_at > current.decided_at:
        return previous
    return current


def _revocable_record_merge_value(current, previous):
    current_revoked_at = getattr(current, "revoked_at", None)
    previous_revoked_at = getattr(previous, "revoked_at", None)
    if current_revoked_at is None and previous_revoked_at is not None:
        return previous
    if current_revoked_at is not None and previous_revoked_at is not None and previous_revoked_at > current_revoked_at:
        return previous
    return current


def _merge_previous_checkpoint_records(store: RuntimeStore, previous: RuntimeStore) -> None:
    dict_attrs = (
        "suspended_turns",
        "approval_requests",
        "permission_grants",
        "boundary_permits",
        "boundary_policy_rules",
    )
    for attr in dict_attrs:
        current = getattr(store, attr)
        older = getattr(previous, attr)
        for key, value in older.items():
            if key not in current:
                if attr == "suspended_turns":
                    approval = store.approval_requests.get(key)
                    if approval is not None and approval.status is not ApprovalStatus.PENDING:
                        continue
                current[key] = value
            elif attr == "approval_requests":
                current[key] = _approval_merge_value(current[key], value)
            elif attr in {"permission_grants", "boundary_permits", "boundary_policy_rules"}:
                current[key] = _revocable_record_merge_value(current[key], value)
    for attr in ("events", "audit_records"):
        _merge_list_records(getattr(store, attr), getattr(previous, attr))

_RUNTIME_STORE_COLLECTION_KEYS = (

    "capsules",
    "events",
    "audit_records",
    "progress_updates",
    "scheduled_tasks",
    "reminders",
    "suspended_turns",
    "doctor_signals",
    "sentinel_signals",
    "sentinel_escalations",
    "approval_requests",
    "permission_grants",
    "boundary_permits",
    "boundary_policy_rules",
    "doctor_recommendations",
    "doctor_actions",
    "mini_agent_runs",
    "missions",
    "builder_proposals",
    "skills",
    "user_facts",
    "preferences",
    "environment_facts",
    "skill_execution_plans",
    "task_frames",
    "active_task_frames",
    "conversation_turns",
    "conversation_branches",
    "conversation_heads",
    "conversation_commits",
    "conversation_ingress_ids",
    "conversation_events",
)

_SQLITE_RUNTIME_TABLES: dict[str, str] = {
    "capsules": "runtime_capsules",
    "events": "runtime_events",
    "audit_records": "runtime_audit_records",
    "progress_updates": "runtime_progress_updates",
    "scheduled_tasks": "reminders_crons",
    "reminders": "reminders_crons",
    "suspended_turns": "runtime_suspended_turns",
    "doctor_signals": "doctor_actions",
    "sentinel_signals": "boundary_rules",
    "sentinel_escalations": "boundary_rules",
    "approval_requests": "approvals",
    "permission_grants": "permission_grants",
    "boundary_permits": "boundary_rules",
    "boundary_policy_rules": "boundary_rules",
    "doctor_recommendations": "doctor_actions",
    "doctor_actions": "doctor_actions",
    "mini_agent_runs": "mini_agent_runs",
    "missions": "tasks_missions",
    "builder_proposals": "builder_proposals",
    "skills": "runtime_skills",
    "user_facts": "memory",
    "preferences": "memory",
    "environment_facts": "memory",
    "skill_execution_plans": "runtime_skills",
    "task_frames": "tasks_missions",
    "active_task_frames": "tasks_missions",
    "conversation_turns": "conversation_events",
    "conversation_branches": "conversation_events",
    "conversation_heads": "conversation_events",
    "conversation_commits": "conversation_events",
    "conversation_ingress_ids": "conversation_events",
    "conversation_events": "conversation_events",
}

_SQLITE_TABLE_NAMES = tuple(dict.fromkeys(_SQLITE_RUNTIME_TABLES.values()))
_SQLITE_RUNTIME_COLLECTIONS_BY_TABLE = {
    table_name: tuple(
        collection
        for collection, collection_table_name in _SQLITE_RUNTIME_TABLES.items()
        if collection_table_name == table_name
    )
    for table_name in _SQLITE_TABLE_NAMES
}
_ENCRYPTED_SQLITE_COLLECTIONS = frozenset({"user_facts", "preferences", "environment_facts"})


def _is_sqlite_runtime_path(path: Path) -> bool:
    return path.suffix.lower() in RUNTIME_SQLITE_SUFFIXES


def _runtime_sqlite_path_for(path: Path) -> Path:
    if _is_sqlite_runtime_path(path):
        return path
    return path.with_name("runtime.db")


def _runtime_memory_key_path(db_path: Path) -> Path:
    explicit = os.environ.get("NULLION_MEMORY_KEY_PATH")
    if explicit:
        return Path(explicit).expanduser()
    if db_path.parent != DEFAULT_NULLION_HOME:
        return db_path.with_name("memory.key")
    return DEFAULT_MEMORY_KEY_PATH


def _runtime_memory_key_storage(db_path: Path) -> str | None:
    raw = os.environ.get("NULLION_KEY_STORAGE")
    if raw:
        return raw
    if db_path.parent != DEFAULT_NULLION_HOME:
        return None
    env_path = DEFAULT_NULLION_HOME / ".env"
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() == "NULLION_KEY_STORAGE":
                return value.strip().strip('"').strip("'")
    except OSError:
        return None
    return None


def _runtime_memory_cipher_for(db_path: Path) -> Fernet:
    key = load_or_create_fernet_key(
        _runtime_memory_key_path(db_path),
        storage=_runtime_memory_key_storage(db_path),
        keychain_service=MEMORY_KEYCHAIN_SERVICE,
        keychain_account=MEMORY_KEYCHAIN_ACCOUNT,
    )
    return Fernet(key)


def _encode_sqlite_runtime_payload(collection: str, checkpoint_path: Path, payload: dict[str, object]) -> str:
    text = json.dumps(payload, sort_keys=True)
    if collection not in _ENCRYPTED_SQLITE_COLLECTIONS:
        return text
    encrypted = _runtime_memory_cipher_for(checkpoint_path).encrypt(text.encode("utf-8")).decode("ascii")
    return _ENCRYPTED_RUNTIME_MEMORY_PREFIX + encrypted


def _encrypt_sqlite_runtime_memory_text(checkpoint_path: Path, text: str) -> str:
    encrypted = _runtime_memory_cipher_for(checkpoint_path).encrypt(text.encode("utf-8")).decode("ascii")
    return _ENCRYPTED_RUNTIME_MEMORY_PREFIX + encrypted


def _decode_sqlite_runtime_payload(collection: str, checkpoint_path: Path, payload: str) -> dict[str, object]:
    if collection not in _ENCRYPTED_SQLITE_COLLECTIONS:
        return json.loads(payload)
    if not payload.startswith(_ENCRYPTED_RUNTIME_MEMORY_PREFIX):
        raise ValueError("Runtime memory payload is not encrypted")
    encrypted = payload.removeprefix(_ENCRYPTED_RUNTIME_MEMORY_PREFIX).encode("ascii")
    decrypted = _runtime_memory_cipher_for(checkpoint_path).decrypt(encrypted).decode("utf-8")
    return json.loads(decrypted)


def _sqlite_memory_migration_failure_ddl() -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {_MEMORY_MIGRATION_FAILURES_TABLE} (
    failure_id     TEXT PRIMARY KEY,
    collection     TEXT NOT NULL,
    original_key   TEXT NOT NULL,
    payload        TEXT NOT NULL,
    reason         TEXT NOT NULL,
    quarantined_at TEXT NOT NULL
);
"""


def _quarantine_sqlite_runtime_row(
    conn: sqlite3.Connection,
    *,
    checkpoint_path: Path,
    table_name: str,
    rowid: int,
    collection: str,
    item_key: str,
    payload: str,
    reason: str,
) -> None:
    """Move an unreadable runtime row aside so runtime startup can continue."""
    if table_name not in _SQLITE_TABLE_NAMES:
        raise ValueError(f"Unknown runtime table: {table_name}")
    now = datetime.now(UTC).isoformat()
    conn.execute("PRAGMA secure_delete=ON")
    conn.executescript(_sqlite_memory_migration_failure_ddl())
    quarantine_payload = payload
    if not payload.startswith(_ENCRYPTED_RUNTIME_MEMORY_PREFIX):
        quarantine_payload = _encrypt_sqlite_runtime_memory_text(checkpoint_path, payload)
    conn.execute(
        f"""INSERT OR REPLACE INTO {_MEMORY_MIGRATION_FAILURES_TABLE}
            (failure_id, collection, original_key, payload, reason, quarantined_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
        (f"{collection}:{rowid}", collection, item_key, quarantine_payload, reason, now),
    )
    conn.execute(f"DELETE FROM {table_name} WHERE rowid = ?", (rowid,))


def migrate_sqlite_runtime_memory_payloads(path: str | Path) -> dict[str, object]:
    """Encrypt legacy plaintext memory rows before the runtime loads them.

    Valid legacy rows are rewritten in place as Fernet payloads. Malformed rows
    are encrypted into a quarantine table and removed from active memory so the
    runtime can still start and the updater can tell the user what happened.
    """
    checkpoint_path = Path(path).expanduser()
    result: dict[str, object] = {
        "attempted": False,
        "encrypted_rows": 0,
        "already_encrypted_rows": 0,
        "quarantined_rows": 0,
        "failures": [],
    }
    if not _is_sqlite_runtime_path(checkpoint_path) or not checkpoint_path.exists():
        return result

    with sqlite3.connect(str(checkpoint_path), timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript(_sqlite_runtime_ddl())
        rows = conn.execute(
            "SELECT rowid, collection, item_key, payload FROM memory "
            "WHERE collection IN (?, ?, ?) ORDER BY rowid",
            tuple(sorted(_ENCRYPTED_SQLITE_COLLECTIONS)),
        ).fetchall()
    if not rows:
        return result

    result["attempted"] = True
    updates: list[tuple[int, str]] = []
    quarantines: list[tuple[int, str, str, str, str]] = []
    failures: list[dict[str, str]] = []
    for row in rows:
        rowid = int(row["rowid"])
        collection = str(row["collection"])
        item_key = str(row["item_key"])
        payload = str(row["payload"])
        if payload.startswith(_ENCRYPTED_RUNTIME_MEMORY_PREFIX):
            result["already_encrypted_rows"] = int(result["already_encrypted_rows"]) + 1
            continue
        try:
            decoded = json.loads(payload)
            if not isinstance(decoded, dict):
                raise ValueError("memory payload is not a JSON object")
            updates.append((rowid, _encode_sqlite_runtime_payload(collection, checkpoint_path, decoded)))
        except Exception as exc:
            reason = str(exc) or exc.__class__.__name__
            failures.append({"collection": collection, "row": str(rowid), "reason": reason})
            quarantines.append(
                (
                    rowid,
                    collection,
                    f"row:{rowid}",
                    _encrypt_sqlite_runtime_memory_text(checkpoint_path, payload),
                    reason,
                )
            )

    if not updates and not quarantines:
        result["failures"] = failures
        return result

    now = datetime.now(UTC).isoformat()
    with sqlite3.connect(str(checkpoint_path), timeout=10) as conn:
        conn.execute("PRAGMA secure_delete=ON")
        conn.executescript(_sqlite_runtime_ddl())
        conn.executescript(_sqlite_memory_migration_failure_ddl())
        for rowid, encrypted_payload in updates:
            conn.execute(
                "UPDATE memory SET item_key = ?, payload = ?, updated_at = ? WHERE rowid = ?",
                (f"encrypted:{rowid}", encrypted_payload, now, rowid),
            )
        for rowid, collection, item_key, encrypted_payload, reason in quarantines:
            _quarantine_sqlite_runtime_row(
                conn,
                checkpoint_path=checkpoint_path,
                table_name="memory",
                rowid=rowid,
                collection=collection,
                item_key=item_key,
                payload=encrypted_payload,
                reason=reason,
            )
    with sqlite3.connect(str(checkpoint_path), timeout=10) as conn:
        conn.execute("VACUUM")

    result["encrypted_rows"] = len(updates)
    result["quarantined_rows"] = len(quarantines)
    result["failures"] = failures
    return result


def _dt(value):
    return None if value is None else value.isoformat()



def _parse_dt(value: str | None):
    return None if value is None else datetime.fromisoformat(value)


def migrate_runtime_store_payload(payload: dict[str, object]) -> dict[str, object]:
    migrated = dict(payload)
    migrated["format_version"] = int(migrated.get("format_version", RUNTIME_STORE_FORMAT_VERSION))
    for key in _RUNTIME_STORE_COLLECTION_KEYS:
        migrated.setdefault(key, [])

    migrated_builder_proposals: list[dict[str, object]] = []
    for proposal in migrated.get("builder_proposals", []):
        if not isinstance(proposal, dict):
            migrated_builder_proposals.append(proposal)
            continue
        proposal_payload = dict(proposal)
        if proposal_payload.get("created_at") is None:
            fallback_created_at = proposal_payload.get("resolved_at") or _BUILDER_PROPOSAL_FALLBACK_CREATED_AT.isoformat()
            proposal_payload["created_at"] = fallback_created_at
        migrated_builder_proposals.append(proposal_payload)
    migrated["builder_proposals"] = migrated_builder_proposals
    return migrated



def _serialize_capsule(capsule: IntentCapsule) -> dict[str, object]:
    return {
        "capsule_id": capsule.capsule_id,
        "owner": capsule.owner,
        "goal": capsule.goal,
        "state": capsule.state.value,
        "risk_level": capsule.risk_level,
        "active_mini_agents": list(capsule.active_mini_agents),
        "pending_approval_id": capsule.pending_approval_id,
        "success_criteria": list(capsule.success_criteria),
    }



def _deserialize_capsule(payload: dict[str, object]) -> IntentCapsule:
    return IntentCapsule(
        capsule_id=str(payload["capsule_id"]),
        owner=str(payload["owner"]),
        goal=str(payload["goal"]),
        state=IntentState(str(payload["state"])),
        risk_level=str(payload["risk_level"]),
        active_mini_agents=list(payload["active_mini_agents"]),
        pending_approval_id=payload.get("pending_approval_id"),
        success_criteria=list(payload["success_criteria"]),
    )



def _serialize_event(event: Event) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "actor": event.actor,
        "created_at": event.created_at.isoformat(),
        "payload": dict(event.payload),
    }



def _deserialize_event(payload: dict[str, object]) -> Event:
    return Event(
        event_id=str(payload["event_id"]),
        event_type=str(payload["event_type"]),
        actor=str(payload["actor"]),
        created_at=_parse_dt(str(payload["created_at"])),
        payload=dict(payload.get("payload", {})),
    )



def _serialize_audit(record: AuditRecord) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "action": record.action,
        "actor": record.actor,
        "created_at": record.created_at.isoformat(),
        "details": dict(record.details),
    }



def _deserialize_audit(payload: dict[str, object]) -> AuditRecord:
    return AuditRecord(
        record_id=str(payload["record_id"]),
        action=str(payload["action"]),
        actor=str(payload["actor"]),
        created_at=_parse_dt(str(payload["created_at"])),
        details=dict(payload.get("details", {})),
    )



def _serialize_progress(update: ProgressUpdate) -> dict[str, object]:
    return {
        "state": update.state.value,
        "message": update.message,
        "capsule_id": update.capsule_id,
    }



def _deserialize_progress(payload: dict[str, object]) -> ProgressUpdate:
    return ProgressUpdate(
        state=ProgressState(str(payload["state"])),
        message=str(payload["message"]),
        capsule_id=str(payload["capsule_id"]),
    )



def _serialize_scheduled_task(task: ScheduledTask) -> dict[str, object]:
    return {
        "task_id": task.task_id,
        "capsule_id": task.capsule_id,
        "schedule_kind": task.schedule_kind.value,
        "interval_minutes": task.interval_minutes,
        "enabled": task.enabled,
        "last_run_at": _dt(task.last_run_at),
        "failure_count": task.failure_count,
    }



def _deserialize_scheduled_task(payload: dict[str, object]) -> ScheduledTask:
    return ScheduledTask(
        task_id=str(payload["task_id"]),
        capsule_id=str(payload["capsule_id"]),
        schedule_kind=ScheduleKind(str(payload["schedule_kind"])),
        interval_minutes=int(payload["interval_minutes"]),
        enabled=bool(payload["enabled"]),
        last_run_at=_parse_dt(payload.get("last_run_at")),
        failure_count=int(payload["failure_count"]),
    )



def _serialize_reminder(reminder: ReminderRecord) -> dict[str, object]:
    return {
        "task_id": reminder.task_id,
        "chat_id": reminder.chat_id,
        "text": reminder.text,
        "due_at": reminder.due_at.isoformat(),
        "delivered_at": _dt(reminder.delivered_at),
    }



def _deserialize_reminder(payload: dict[str, object]) -> ReminderRecord:
    return ReminderRecord(
        task_id=str(payload["task_id"]),
        chat_id=str(payload["chat_id"]),
        text=str(payload["text"]),
        due_at=_parse_dt(str(payload["due_at"])),
        delivered_at=_parse_dt(payload.get("delivered_at")),
    )


def _serialize_suspended_turn(suspended_turn: SuspendedTurn) -> dict[str, object]:
    return {
        "approval_id": suspended_turn.approval_id,
        "conversation_id": suspended_turn.conversation_id,
        "chat_id": suspended_turn.chat_id,
        "message": suspended_turn.message,
        "request_id": suspended_turn.request_id,
        "message_id": suspended_turn.message_id,
        "created_at": suspended_turn.created_at.isoformat(),
        "mission_id": suspended_turn.mission_id,
        "pending_step_idx": suspended_turn.pending_step_idx,
        "messages_snapshot": suspended_turn.messages_snapshot,
        "pending_tool_calls": suspended_turn.pending_tool_calls,
        "task_id": suspended_turn.task_id,
        "group_id": suspended_turn.group_id,
        "agent_id": suspended_turn.agent_id,
        "resume_token": suspended_turn.resume_token,
    }


def _deserialize_suspended_turn(payload: dict[str, object]) -> SuspendedTurn:
    return SuspendedTurn(
        approval_id=str(payload["approval_id"]),
        conversation_id=str(payload["conversation_id"]),
        chat_id=None if payload.get("chat_id") is None else str(payload.get("chat_id")),
        message=str(payload["message"]),
        request_id=None if payload.get("request_id") is None else str(payload.get("request_id")),
        message_id=None if payload.get("message_id") is None else str(payload.get("message_id")),
        created_at=_parse_dt(str(payload["created_at"])),
        mission_id=None if payload.get("mission_id") is None else str(payload.get("mission_id")),
        pending_step_idx=None if payload.get("pending_step_idx") is None else int(payload.get("pending_step_idx")),
        messages_snapshot=payload.get("messages_snapshot"),
        pending_tool_calls=payload.get("pending_tool_calls"),
        task_id=None if payload.get("task_id") is None else str(payload.get("task_id")),
        group_id=None if payload.get("group_id") is None else str(payload.get("group_id")),
        agent_id=None if payload.get("agent_id") is None else str(payload.get("agent_id")),
        resume_token=payload.get("resume_token") if isinstance(payload.get("resume_token"), dict) else None,
    )


def _serialize_signal(route: SignalRoute) -> dict[str, object]:
    return {
        "target": route.target.value,
        "reason": route.reason,
        "severity": route.severity,
        "summary": route.summary,
        "error": route.error,
        "recommendation_code": route.recommendation_code,
    }



def _deserialize_signal(payload: dict[str, object]) -> SignalRoute:
    return SignalRoute(
        target=SignalTarget(str(payload["target"])),
        reason=str(payload["reason"]),
        severity=str(payload["severity"]),
        summary=None if payload.get("summary") is None else str(payload.get("summary")),
        error=None if payload.get("error") is None else str(payload.get("error")),
        recommendation_code=None
        if payload.get("recommendation_code") is None
        else str(payload.get("recommendation_code")),
    )



def _serialize_escalation(escalation: SentinelEscalationArtifact) -> dict[str, object]:
    return {
        "escalation_id": escalation.escalation_id,
        "source_signal_reason": escalation.source_signal_reason,
        "severity": escalation.severity,
        "status": escalation.status.value,
        "created_at": escalation.created_at.isoformat(),
        "summary": escalation.summary,
        "approval_id": escalation.approval_id,
    }



def _deserialize_escalation(payload: dict[str, object]) -> SentinelEscalationArtifact:
    return SentinelEscalationArtifact(
        escalation_id=str(payload["escalation_id"]),
        source_signal_reason=str(payload["source_signal_reason"]),
        severity=str(payload["severity"]),
        status=EscalationStatus(str(payload["status"])),
        created_at=_parse_dt(str(payload["created_at"])),
        summary=str(payload["summary"]),
        approval_id=payload.get("approval_id"),
    )



def _serialize_approval(approval: ApprovalRequest) -> dict[str, object]:
    return {
        "approval_id": approval.approval_id,
        "requested_by": approval.requested_by,
        "action": approval.action,
        "resource": approval.resource,
        "status": approval.status.value,
        "created_at": approval.created_at.isoformat(),
        "request_kind": approval.request_kind,
        "context": approval.context,
        "decided_by": approval.decided_by,
        "decided_at": _dt(approval.decided_at),
        "decision_reason": approval.decision_reason,
    }


def _deserialize_approval(payload: dict[str, object]) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=str(payload["approval_id"]),
        requested_by=str(payload["requested_by"]),
        action=str(payload["action"]),
        resource=str(payload["resource"]),
        status=ApprovalStatus(str(payload["status"])),
        created_at=_parse_dt(str(payload["created_at"])),
        request_kind=str(payload.get("request_kind", "capability_grant")),
        context=dict(payload["context"]) if isinstance(payload.get("context"), dict) else None,
        decided_by=payload.get("decided_by"),
        decided_at=_parse_dt(payload.get("decided_at")),
        decision_reason=payload.get("decision_reason"),
    )


def _serialize_permission_grant(grant: PermissionGrant) -> dict[str, object]:
    return {
        "grant_id": grant.grant_id,
        "approval_id": grant.approval_id,
        "principal_id": grant.principal_id,
        "permission": grant.permission,
        "granted_by": grant.granted_by,
        "granted_at": grant.granted_at.isoformat(),
        "expires_at": _dt(grant.expires_at),
        "revoked_by": grant.revoked_by,
        "revoked_at": _dt(grant.revoked_at),
        "revoked_reason": grant.revoked_reason,
    }


def _deserialize_permission_grant(payload: dict[str, object]) -> PermissionGrant:
    return PermissionGrant(
        grant_id=str(payload["grant_id"]),
        approval_id=str(payload["approval_id"]),
        principal_id=str(payload["principal_id"]),
        permission=str(payload["permission"]),
        granted_by=str(payload["granted_by"]),
        granted_at=_parse_dt(str(payload["granted_at"])),
        expires_at=_parse_dt(payload.get("expires_at")),
        revoked_by=payload.get("revoked_by"),
        revoked_at=_parse_dt(payload.get("revoked_at")),
        revoked_reason=payload.get("revoked_reason"),
    )


def _serialize_boundary_permit(permit: BoundaryPermit) -> dict[str, object]:
    return {
        "permit_id": permit.permit_id,
        "approval_id": permit.approval_id,
        "principal_id": permit.principal_id,
        "boundary_kind": permit.boundary_kind.value,
        "selector": permit.selector,
        "granted_by": permit.granted_by,
        "granted_at": permit.granted_at.isoformat(),
        "uses_remaining": permit.uses_remaining,
        "expires_at": _dt(permit.expires_at),
        "revoked_by": permit.revoked_by,
        "revoked_at": _dt(permit.revoked_at),
        "revoked_reason": permit.revoked_reason,
    }


def _deserialize_boundary_permit(payload: dict[str, object]) -> BoundaryPermit:
    return BoundaryPermit(
        permit_id=str(payload["permit_id"]),
        approval_id=str(payload["approval_id"]),
        principal_id=str(payload["principal_id"]),
        boundary_kind=BoundaryKind(str(payload["boundary_kind"])),
        selector=str(payload["selector"]),
        granted_by=str(payload["granted_by"]),
        granted_at=_parse_dt(str(payload["granted_at"])),
        uses_remaining=int(payload.get("uses_remaining", 1)),
        expires_at=_parse_dt(payload.get("expires_at")),
        revoked_by=payload.get("revoked_by"),
        revoked_at=_parse_dt(payload.get("revoked_at")),
        revoked_reason=payload.get("revoked_reason"),
    )


def _serialize_boundary_policy_rule(rule: BoundaryPolicyRule) -> dict[str, object]:
    return {
        "rule_id": rule.rule_id,
        "principal_id": rule.principal_id,
        "kind": rule.kind.value,
        "mode": rule.mode.value,
        "selector": rule.selector,
        "created_by": rule.created_by,
        "created_at": rule.created_at.isoformat(),
        "priority": rule.priority,
        "reason": rule.reason,
        "expires_at": _dt(rule.expires_at),
        "revoked_at": _dt(rule.revoked_at),
    }


def _deserialize_boundary_policy_rule(payload: dict[str, object]) -> BoundaryPolicyRule:
    return BoundaryPolicyRule(
        rule_id=str(payload["rule_id"]),
        principal_id=str(payload["principal_id"]),
        kind=BoundaryKind(str(payload["kind"])),
        mode=PolicyMode(str(payload["mode"])),
        selector=str(payload["selector"]),
        created_by=str(payload["created_by"]),
        created_at=_parse_dt(str(payload["created_at"])),
        priority=int(payload.get("priority", 0)),
        reason=payload.get("reason"),
        expires_at=_parse_dt(payload.get("expires_at")),
        revoked_at=_parse_dt(payload.get("revoked_at")),
    )


def _serialize_mini_agent_run(run: MiniAgentRun) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "capsule_id": run.capsule_id,
        "mini_agent_type": run.mini_agent_type,
        "status": run.status.value,
        "created_at": run.created_at.isoformat(),
        "result_summary": run.result_summary,
    }



def _deserialize_mini_agent_run(payload: dict[str, object]) -> MiniAgentRun:
    return MiniAgentRun(
        run_id=str(payload["run_id"]),
        capsule_id=str(payload["capsule_id"]),
        mini_agent_type=str(payload["mini_agent_type"]),
        status=MiniAgentRunStatus(str(payload["status"])),
        created_at=_parse_dt(str(payload["created_at"])),
        result_summary=payload.get("result_summary"),
    )



def _serialize_mission_step(step: MissionStep) -> dict[str, object]:
    return {
        "step_id": step.step_id,
        "title": step.title,
        "status": step.status,
        "kind": step.kind,
        "capsule_id": step.capsule_id,
        "mini_agent_run_id": step.mini_agent_run_id,
        "mini_agent_run_ids": list(step.mini_agent_run_ids),
        "required_mini_agent_run_ids": list(step.required_mini_agent_run_ids),
        "notes": step.notes,
        "metadata": step.metadata,
    }



def _normalize_mission_step_metadata(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            continue
        normalized[key] = item
    return normalized



def _deserialize_mission_step(payload: dict[str, object]) -> MissionStep:
    return MissionStep(
        step_id=str(payload["step_id"]),
        title=str(payload["title"]),
        status=str(payload["status"]),
        kind=str(payload["kind"]),
        capsule_id=payload.get("capsule_id"),
        mini_agent_run_id=payload.get("mini_agent_run_id"),
        mini_agent_run_ids=tuple(str(run_id) for run_id in payload.get("mini_agent_run_ids", [])),
        required_mini_agent_run_ids=tuple(
            str(run_id) for run_id in payload.get("required_mini_agent_run_ids", [])
        ),
        notes=payload.get("notes"),
        metadata=_normalize_mission_step_metadata(payload.get("metadata")),
    )



def _serialize_mission_checklist_item(item: MissionChecklistItem) -> dict[str, object]:
    return {
        "item_id": item.item_id,
        "label": item.label,
        "required": item.required,
        "satisfied": item.satisfied,
        "details": item.details,
    }



def _deserialize_mission_checklist_item(payload: dict[str, object]) -> MissionChecklistItem:
    return MissionChecklistItem(
        item_id=str(payload["item_id"]),
        label=str(payload["label"]),
        required=bool(payload.get("required", True)),
        satisfied=bool(payload.get("satisfied", False)),
        details=payload.get("details"),
    )



def _serialize_mission(mission: MissionRecord) -> dict[str, object]:
    return {
        "mission_id": mission.mission_id,
        "owner": mission.owner,
        "title": mission.title,
        "goal": mission.goal,
        "status": mission.status.value,
        "continuation_policy": mission.continuation_policy.value,
        "created_from_capsule_id": mission.created_from_capsule_id,
        "active_capsule_id": mission.active_capsule_id,
        "active_step_id": mission.active_step_id,
        "steps": [_serialize_mission_step(step) for step in mission.steps],
        "completion_checklist": [
            _serialize_mission_checklist_item(item) for item in mission.completion_checklist
        ],
        "blocked_reason": mission.blocked_reason,
        "waiting_on": mission.waiting_on,
        "result_summary": mission.result_summary,
        "terminal_reason": None if mission.terminal_reason is None else mission.terminal_reason.value,
        "last_progress_message": mission.last_progress_message,
        "created_at": _dt(mission.created_at),
        "updated_at": _dt(mission.updated_at),
    }



def _deserialize_mission(payload: dict[str, object]) -> MissionRecord:
    terminal_reason = payload.get("terminal_reason")
    return MissionRecord(
        mission_id=str(payload["mission_id"]),
        owner=str(payload["owner"]),
        title=str(payload["title"]),
        goal=str(payload["goal"]),
        status=MissionStatus(str(payload["status"])),
        continuation_policy=MissionContinuationPolicy(str(payload["continuation_policy"])),
        created_from_capsule_id=payload.get("created_from_capsule_id"),
        active_capsule_id=payload.get("active_capsule_id"),
        active_step_id=payload.get("active_step_id"),
        steps=tuple(_deserialize_mission_step(dict(step)) for step in payload.get("steps", [])),
        completion_checklist=tuple(
            _deserialize_mission_checklist_item(dict(item))
            for item in payload.get("completion_checklist", [])
        ),
        blocked_reason=payload.get("blocked_reason"),
        waiting_on=payload.get("waiting_on"),
        result_summary=payload.get("result_summary"),
        terminal_reason=None if terminal_reason is None else MissionTerminalReason(str(terminal_reason)),
        last_progress_message=payload.get("last_progress_message"),
        created_at=_parse_dt(payload.get("created_at")),
        updated_at=_parse_dt(payload.get("updated_at")),
    )



def _serialize_builder_proposal(proposal: BuilderProposal) -> dict[str, object]:
    return {
        "decision_type": proposal.decision_type.value,
        "title": proposal.title,
        "summary": proposal.summary,
        "confidence": proposal.confidence,
        "approval_mode": proposal.approval_mode,
        "suggested_skill_title": proposal.suggested_skill_title,
        "suggested_trigger": proposal.suggested_trigger,
        "suggested_steps": list(proposal.suggested_steps),
        "suggested_tags": list(proposal.suggested_tags),
    }



def _deserialize_builder_proposal(payload: dict[str, object]) -> BuilderProposal:
    return BuilderProposal(
        decision_type=BuilderDecisionType(str(payload["decision_type"])),
        title=str(payload["title"]),
        summary=str(payload["summary"]),
        confidence=float(payload["confidence"]),
        approval_mode=str(payload["approval_mode"]),
        suggested_skill_title=payload.get("suggested_skill_title"),
        suggested_trigger=payload.get("suggested_trigger"),
        suggested_steps=tuple(payload.get("suggested_steps", [])),
        suggested_tags=tuple(payload.get("suggested_tags", [])),
    )



def _serialize_builder_proposal_record(record: BuilderProposalRecord) -> dict[str, object]:
    return {
        "proposal_id": record.proposal_id,
        "proposal": _serialize_builder_proposal(record.proposal),
        "status": record.status,
        "created_at": _dt(record.created_at),
        "accepted_skill_id": record.accepted_skill_id,
        "resolved_at": _dt(record.resolved_at),
        "context_key": record.context_key,
    }



def _deserialize_builder_proposal_record(payload: dict[str, object]) -> BuilderProposalRecord:
    raw_created_at = payload.get("created_at")
    if raw_created_at is None:
        raw_created_at = payload.get("resolved_at")
    created_at = _parse_dt(raw_created_at)
    if created_at is None:
        created_at = _BUILDER_PROPOSAL_FALLBACK_CREATED_AT
    return BuilderProposalRecord(
        proposal_id=str(payload["proposal_id"]),
        proposal=_deserialize_builder_proposal(dict(payload["proposal"])),
        status=str(payload["status"]),
        created_at=created_at,
        accepted_skill_id=payload.get("accepted_skill_id"),
        resolved_at=_parse_dt(payload.get("resolved_at")),
        context_key=payload.get("context_key"),
    )



def _serialize_skill_revision(revision: SkillRevision) -> dict[str, object]:
    return {
        "revision": revision.revision,
        "title": revision.title,
        "summary": revision.summary,
        "trigger": revision.trigger,
        "steps": list(revision.steps),
        "tags": list(revision.tags),
        "updated_at": _dt(revision.updated_at),
    }


def _deserialize_skill_revision(payload: dict[str, object]) -> SkillRevision:
    return SkillRevision(
        revision=int(payload.get("revision", 1)),
        title=str(payload["title"]),
        summary=str(payload["summary"]),
        trigger=str(payload["trigger"]),
        steps=list(payload.get("steps", [])),
        tags=list(payload.get("tags", [])),
        updated_at=_parse_dt(payload.get("updated_at")),
    )


def _serialize_skill_workflow_signal(signal: SkillWorkflowSignal) -> dict[str, object]:
    return {
        "source": signal.source,
        "summary": signal.summary,
        "recorded_at": _dt(signal.recorded_at),
    }


def _deserialize_skill_workflow_signal(payload: dict[str, object]) -> SkillWorkflowSignal:
    return SkillWorkflowSignal(
        source=str(payload["source"]),
        summary=str(payload["summary"]),
        recorded_at=_parse_dt(payload.get("recorded_at")),
    )


def _serialize_skill(skill: SkillRecord) -> dict[str, object]:
    return {
        "skill_id": skill.skill_id,
        "title": skill.title,
        "summary": skill.summary,
        "trigger": skill.trigger,
        "steps": list(skill.steps),
        "tags": list(skill.tags),
        "created_at": _dt(skill.created_at),
        "updated_at": _dt(skill.updated_at),
        "revision": skill.revision,
        "revision_history": [_serialize_skill_revision(revision) for revision in skill.revision_history],
        "workflow_signals": [_serialize_skill_workflow_signal(signal) for signal in skill.workflow_signals],
    }



def _deserialize_skill(payload: dict[str, object]) -> SkillRecord:
    return SkillRecord(
        skill_id=str(payload["skill_id"]),
        title=str(payload["title"]),
        summary=str(payload["summary"]),
        trigger=str(payload["trigger"]),
        steps=list(payload.get("steps", [])),
        tags=list(payload.get("tags", [])),
        created_at=_parse_dt(payload.get("created_at")),
        updated_at=_parse_dt(payload.get("updated_at")),
        revision=int(payload.get("revision", 1)),
        revision_history=[
            _deserialize_skill_revision(dict(revision_payload))
            for revision_payload in payload.get("revision_history", [])
        ],
        workflow_signals=[
            _deserialize_skill_workflow_signal(dict(signal_payload))
            for signal_payload in payload.get("workflow_signals", [])
        ],
    )



def _serialize_user_memory_entry(entry: UserMemoryEntry) -> dict[str, object]:
    return {
        "entry_id": entry.entry_id,
        "owner": entry.owner,
        "kind": entry.kind.value,
        "key": entry.key,
        "value": entry.value,
        "source": entry.source,
        "created_at": _dt(entry.created_at),
        "updated_at": _dt(entry.updated_at),
        "use_count": int(getattr(entry, "use_count", 0) or 0),
        "use_score": float(getattr(entry, "use_score", 0.0) or 0.0),
        "last_used_at": _dt(getattr(entry, "last_used_at", None)),
    }



def _deserialize_user_memory_entry(payload: dict[str, object]) -> UserMemoryEntry:
    return UserMemoryEntry(
        entry_id=str(payload["entry_id"]),
        owner=str(payload["owner"]),
        kind=UserMemoryKind(str(payload["kind"])),
        key=str(payload["key"]),
        value=str(payload["value"]),
        source=payload.get("source"),
        created_at=_parse_dt(payload.get("created_at")),
        updated_at=_parse_dt(payload.get("updated_at")),
        use_count=int(payload.get("use_count") or 0),
        use_score=float(payload.get("use_score") or 0.0),
        last_used_at=_parse_dt(payload.get("last_used_at")),
    )




def _serialize_skill_execution_plan(capsule_id: str, plan: SkillExecutionPlan) -> dict[str, object]:
    return {
        "capsule_id": capsule_id,
        "plan_id": plan.plan_id,
        "skill_id": plan.skill_id,
        "title": plan.title,
        "steps": list(plan.steps),
        "safety_mode": plan.safety_mode,
        "step_states": list(plan.step_states),
        "active_step_index": plan.active_step_index,
    }



def _deserialize_skill_execution_plan(payload: dict[str, object]) -> tuple[str, SkillExecutionPlan]:
    capsule_id = str(payload["capsule_id"])
    plan = SkillExecutionPlan(
        plan_id=str(payload["plan_id"]),
        skill_id=str(payload["skill_id"]),
        title=str(payload["title"]),
        steps=tuple(str(step) for step in payload.get("steps", [])),
        safety_mode=str(payload.get("safety_mode", "suggest_only")),
        step_states=tuple(str(state) for state in payload.get("step_states", [])),
        active_step_index=payload.get("active_step_index"),
    )
    return capsule_id, plan


def _serialize_enum_like(value: object) -> object:
    return getattr(value, "value", value)


def _serialize_task_frame_target(target: TaskFrameTarget | None) -> dict[str, object] | None:
    if target is None:
        return None
    return {
        "kind": target.kind,
        "value": target.value,
        "normalized_value": target.normalized_value,
    }


def _deserialize_task_frame_target(payload: dict[str, object] | None) -> TaskFrameTarget | None:
    if payload is None:
        return None
    return TaskFrameTarget(
        kind=str(payload["kind"]),
        value=str(payload["value"]),
        normalized_value=payload.get("normalized_value"),
    )


def _serialize_task_frame(frame: TaskFrame) -> dict[str, object]:
    return {
        "frame_id": frame.frame_id,
        "conversation_id": frame.conversation_id,
        "branch_id": frame.branch_id,
        "source_turn_id": frame.source_turn_id,
        "parent_frame_id": frame.parent_frame_id,
        "status": frame.status.value,
        "operation": frame.operation.value,
        "target": _serialize_task_frame_target(frame.target),
        "execution": {
            "preferred_tool_family": frame.execution.preferred_tool_family,
            "fallback_tool_family": frame.execution.fallback_tool_family,
            "approval_sensitive": frame.execution.approval_sensitive,
            "boundary_kind": frame.execution.boundary_kind,
        },
        "output": {
            "artifact_kind": frame.output.artifact_kind,
            "delivery_mode": frame.output.delivery_mode,
            "response_shape": frame.output.response_shape,
        },
        "finish": {
            "requires_attempt": frame.finish.requires_attempt,
            "requires_artifact_delivery": frame.finish.requires_artifact_delivery,
            "required_artifact_kind": frame.finish.required_artifact_kind,
            "required_tool_completion": list(frame.finish.required_tool_completion),
        },
        "summary": frame.summary,
        "created_at": frame.created_at.isoformat(),
        "updated_at": frame.updated_at.isoformat(),
        "last_activity_turn_id": frame.last_activity_turn_id,
        "completion_turn_id": frame.completion_turn_id,
        "metadata": dict(frame.metadata),
    }


def _deserialize_task_frame(payload: dict[str, object]) -> TaskFrame:
    execution_payload = dict(payload.get("execution") or {})
    output_payload = dict(payload.get("output") or {})
    finish_payload = dict(payload.get("finish") or {})
    raw_target = payload.get("target")
    target_payload = dict(raw_target) if isinstance(raw_target, dict) else None
    return TaskFrame(
        frame_id=str(payload["frame_id"]),
        conversation_id=str(payload["conversation_id"]),
        branch_id=str(payload["branch_id"]),
        source_turn_id=str(payload["source_turn_id"]),
        parent_frame_id=payload.get("parent_frame_id"),
        status=TaskFrameStatus(str(payload["status"])),
        operation=TaskFrameOperation(str(payload["operation"])),
        target=_deserialize_task_frame_target(target_payload),
        execution=TaskFrameExecutionContract(
            preferred_tool_family=execution_payload.get("preferred_tool_family"),
            fallback_tool_family=execution_payload.get("fallback_tool_family"),
            approval_sensitive=bool(execution_payload.get("approval_sensitive", False)),
            boundary_kind=execution_payload.get("boundary_kind"),
        ),
        output=TaskFrameOutputContract(
            artifact_kind=output_payload.get("artifact_kind"),
            delivery_mode=output_payload.get("delivery_mode"),
            response_shape=output_payload.get("response_shape"),
        ),
        finish=TaskFrameFinishCriteria(
            requires_attempt=bool(finish_payload.get("requires_attempt", True)),
            requires_artifact_delivery=bool(finish_payload.get("requires_artifact_delivery", False)),
            required_artifact_kind=finish_payload.get("required_artifact_kind"),
            required_tool_completion=tuple(str(name) for name in finish_payload.get("required_tool_completion", [])),
        ),
        summary=str(payload["summary"]),
        created_at=_parse_dt(str(payload["created_at"])),
        updated_at=_parse_dt(str(payload["updated_at"])),
        last_activity_turn_id=payload.get("last_activity_turn_id"),
        completion_turn_id=payload.get("completion_turn_id"),
        metadata=dict(payload.get("metadata") or {}),
    )


def _serialize_active_task_frame(conversation_id: str, frame_id: str) -> dict[str, object]:
    return {
        "conversation_id": conversation_id,
        "frame_id": frame_id,
    }


def _deserialize_active_task_frame(payload: dict[str, object]) -> tuple[str, str]:
    return str(payload["conversation_id"]), str(payload["frame_id"])


def _serialize_conversation_turn(turn: ConversationTurn) -> dict[str, object]:
    return {
        "turn_id": turn.turn_id,
        "conversation_id": turn.conversation_id,
        "branch_id": turn.branch_id,
        "parent_turn_id": turn.parent_turn_id,
        "disposition": _serialize_enum_like(turn.disposition),
        "user_message": turn.user_message,
        "status": turn.status,
        "created_at": turn.created_at.isoformat(),
        "disposition_reason": turn.disposition_reason,
        "started_snapshot_summary": turn.started_snapshot_summary,
        "cancellation_token": turn.cancellation_token,
    }


def _deserialize_conversation_turn(payload: dict[str, object]) -> ConversationTurn:
    return ConversationTurn(
        turn_id=str(payload["turn_id"]),
        conversation_id=str(payload["conversation_id"]),
        branch_id=str(payload["branch_id"]),
        parent_turn_id=payload.get("parent_turn_id"),
        disposition=ConversationTurnDisposition(str(payload["disposition"])),
        user_message=str(payload["user_message"]),
        status=str(payload["status"]),
        created_at=_parse_dt(str(payload["created_at"])),
        disposition_reason=payload.get("disposition_reason"),
        started_snapshot_summary=payload.get("started_snapshot_summary"),
        cancellation_token=payload.get("cancellation_token"),
    )


def _serialize_conversation_branch(branch: ConversationBranch) -> dict[str, object]:
    return {
        "branch_id": branch.branch_id,
        "conversation_id": branch.conversation_id,
        "status": _serialize_enum_like(branch.status),
        "created_from_turn_id": branch.created_from_turn_id,
        "superseded_by_branch_id": branch.superseded_by_branch_id,
        "cancelled_at": _dt(branch.cancelled_at),
    }


def _deserialize_conversation_branch(payload: dict[str, object]) -> ConversationBranch:
    return ConversationBranch(
        branch_id=str(payload["branch_id"]),
        conversation_id=str(payload["conversation_id"]),
        status=ConversationBranchStatus(str(payload["status"])),
        created_from_turn_id=str(payload["created_from_turn_id"]),
        superseded_by_branch_id=payload.get("superseded_by_branch_id"),
        cancelled_at=_parse_dt(payload.get("cancelled_at")),
    )


def _serialize_conversation_head(conversation_id: str, head: dict[str, str | None]) -> dict[str, object]:
    return {
        "conversation_id": conversation_id,
        "active_branch_id": head.get("active_branch_id"),
        "active_turn_id": head.get("active_turn_id"),
    }


def _deserialize_conversation_head(payload: dict[str, object]) -> tuple[str, str | None, str | None]:
    return (
        str(payload["conversation_id"]),
        payload.get("active_branch_id"),
        payload.get("active_turn_id"),
    )


def _serialize_conversation_commit(conversation_id: str, idempotency_keys: set[str]) -> dict[str, object]:
    return {
        "conversation_id": conversation_id,
        "idempotency_keys": sorted(idempotency_keys),
    }


def _deserialize_conversation_commit(payload: dict[str, object]) -> tuple[str, list[str]]:
    conversation_id = str(payload["conversation_id"])
    keys = [str(key) for key in payload.get("idempotency_keys", [])]
    return conversation_id, keys


def _serialize_conversation_ingress_ids(conversation_id: str, ingress_ids: set[str]) -> dict[str, object]:
    return {
        "conversation_id": conversation_id,
        "ingress_ids": sorted(ingress_ids),
    }


def _deserialize_conversation_ingress_ids(payload: dict[str, object]) -> tuple[str, list[str]]:
    conversation_id = str(payload["conversation_id"])
    ingress_ids = [str(ingress_id) for ingress_id in payload.get("ingress_ids", [])]
    return conversation_id, ingress_ids



def _backup_path_for(target: Path, generation: int) -> Path:
    suffix = ".bak" if generation == 0 else f".bak.{generation}"
    return target.with_name(f"{target.name}{suffix}")


def _looks_like_sqlite_runtime_file(path: Path) -> bool:
    name = path.name.lower()
    return (
        path.suffix.lower() in RUNTIME_SQLITE_SUFFIXES
        or ".db." in name
        or ".sqlite." in name
        or ".sqlite3." in name
    )


def _sqlite_quick_check(path: Path) -> str | None:
    if not _looks_like_sqlite_runtime_file(path):
        return None
    try:
        with sqlite3.connect(str(path), timeout=10) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
    except sqlite3.Error as exc:
        return str(exc) or exc.__class__.__name__
    return str(row[0]) if row else "no result"


def _runtime_backup_record(
    checkpoint_path: Path,
    candidate: Path,
    *,
    restore_id: str,
    generation: int | None,
    kind: str,
) -> dict[str, object]:
    try:
        stat = candidate.stat()
        size_bytes = stat.st_size
        modified_at = stat.st_mtime
    except OSError:
        size_bytes = 0
        modified_at = None
    integrity = _sqlite_quick_check(candidate)
    return {
        "generation": generation,
        "restore_id": restore_id,
        "name": candidate.name,
        "path": str(candidate),
        "kind": kind,
        "size_bytes": size_bytes,
        "modified_at": modified_at,
        "integrity": integrity,
        "restorable": candidate.exists() and (integrity in {None, "ok"}),
        "checkpoint": checkpoint_path.name,
    }


def _restore_candidate_for_token(target: Path, token: int | str) -> Path:
    if isinstance(token, int):
        return _backup_path_for(target, token)
    normalized = str(token).strip()
    if not normalized:
        raise FileNotFoundError(target.with_name(f"{target.name}."))
    if normalized.isdigit():
        return _backup_path_for(target, int(normalized))
    allowed_prefixes = ("corrupt-", "pre-smart-restore-")
    if not normalized.startswith(allowed_prefixes):
        raise FileNotFoundError(target.with_name(f"{target.name}.{normalized}"))
    candidate = target.with_name(f"{target.name}.{normalized}")
    if candidate.name != f"{target.name}.{normalized}":
        raise FileNotFoundError(candidate)
    return candidate



def _rotate_runtime_store_backups(target: Path) -> None:
    for generation in range(RUNTIME_STORE_BACKUP_DEPTH - 1, -1, -1):
        source = _backup_path_for(target, generation)
        if not source.exists():
            continue
        if generation == RUNTIME_STORE_BACKUP_DEPTH - 1:
            source.unlink()
            continue
        source.replace(_backup_path_for(target, generation + 1))


def _sqlite_runtime_ddl() -> str:
    table_blocks = [
        f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    collection TEXT NOT NULL,
    item_key   TEXT NOT NULL,
    payload    TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (collection, item_key)
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_collection
    ON {table_name} (collection);
"""
        for table_name in _SQLITE_TABLE_NAMES
    ]
    runtime_event_indexes = """
CREATE INDEX IF NOT EXISTS idx_runtime_events_collection_updated
    ON runtime_events (collection, updated_at);
CREATE INDEX IF NOT EXISTS idx_runtime_events_type_created
    ON runtime_events (
        json_extract(payload, '$.event_type'),
        json_extract(payload, '$.created_at')
    );
CREATE INDEX IF NOT EXISTS idx_runtime_events_conversation_created
    ON runtime_events (
        json_extract(payload, '$.payload.conversation_id'),
        json_extract(payload, '$.created_at')
    );
CREATE INDEX IF NOT EXISTS idx_runtime_events_tool_created
    ON runtime_events (
        json_extract(payload, '$.payload.tool_name'),
        json_extract(payload, '$.created_at')
    );
CREATE INDEX IF NOT EXISTS idx_conversation_events_conversation_created
    ON conversation_events (
        json_extract(payload, '$.conversation_id'),
        json_extract(payload, '$.created_at')
    );
CREATE INDEX IF NOT EXISTS idx_conversation_events_type_created
    ON conversation_events (
        json_extract(payload, '$.event_type'),
        json_extract(payload, '$.created_at')
    );
"""
    return """
CREATE TABLE IF NOT EXISTS runtime_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
""" + "\n".join(table_blocks) + runtime_event_indexes


def _log_runtime_sqlite_timing(
    operation: str,
    started_at: float,
    path: Path,
    *,
    records: int | None = None,
) -> None:
    if not RUNTIME_SQLITE_MEASURE_ENABLED:
        return
    elapsed_ms = (perf_counter() - started_at) * 1000
    extra = ""
    if records is not None:
        extra += " records=%s" % records
    try:
        size = path.stat().st_size
    except OSError:
        size = 0
    message = (
        "runtime SQLite %s took %.1fms path=%s size_bytes=%s%s"
        % (operation, elapsed_ms, path.name, size, extra)
    )
    if elapsed_ms >= RUNTIME_SQLITE_SLOW_MS:
        logger.warning(message)
    else:
        logger.debug(message)


def _sqlite_collection_item_key(collection: str, index: int, item: object) -> str:
    if collection in _ENCRYPTED_SQLITE_COLLECTIONS:
        return f"{index:012d}"
    if isinstance(item, dict):
        for key in (
            "approval_id",
            "grant_id",
            "permit_id",
            "rule_id",
            "action_id",
            "proposal_id",
            "task_id",
            "mission_id",
            "entry_id",
            "turn_id",
            "branch_id",
            "conversation_id",
            "run_id",
            "capsule_id",
            "skill_id",
            "frame_id",
            "event_id",
            "record_id",
        ):
            value = item.get(key)
            if isinstance(value, str) and value:
                return value
    return f"{index:012d}"


def _validate_sqlite_runtime_row(collection: str, decoded: dict[str, object]) -> None:
    payload: dict[str, object] = {
        "format_version": RUNTIME_STORE_FORMAT_VERSION,
        **{key: [] for key in _RUNTIME_STORE_COLLECTION_KEYS},
    }
    payload[collection] = [decoded]
    _runtime_store_from_payload(payload)


def _save_runtime_store_sqlite(store: RuntimeStore, path: str | Path) -> Path:
    target = Path(path)
    started_at = perf_counter()
    if target.exists():
        try:
            previous_store = _load_runtime_store_sqlite(target)
            _merge_previous_checkpoint_records(store, previous_store)
        except Exception:
            pass
    payload = build_runtime_store_payload(store)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _rotate_runtime_store_backups(target)
        shutil.copy2(target, _backup_path_for(target, 0))

    now = datetime.now(UTC).isoformat()
    with sqlite3.connect(str(target), timeout=10) as conn:
        # Keep runtime checkpoints self-contained so backup/restore can copy one file.
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_sqlite_runtime_ddl())
        conn.execute(
            "INSERT OR REPLACE INTO runtime_meta (key, value) VALUES (?, ?)",
            ("format_version", str(RUNTIME_STORE_FORMAT_VERSION)),
        )
        for table_name, collections in _SQLITE_RUNTIME_COLLECTIONS_BY_TABLE.items():
            placeholders = ", ".join("?" for _ in collections)
            conn.execute(f"DELETE FROM {table_name} WHERE collection IN ({placeholders})", collections)
        for collection in _RUNTIME_STORE_COLLECTION_KEYS:
            table_name = _SQLITE_RUNTIME_TABLES[collection]
            rows = payload.get(collection, [])
            if not isinstance(rows, list):
                rows = []
            for index, row in enumerate(rows):
                conn.execute(
                    f"""INSERT OR REPLACE INTO {table_name}
                        (collection, item_key, payload, updated_at)
                        VALUES (?, ?, ?, ?)""",
                    (
                        collection,
                        _sqlite_collection_item_key(collection, index, row),
                        _encode_sqlite_runtime_payload(collection, target, row),
                        now,
                    ),
                )
    record_count = sum(
        len(rows) if isinstance(rows, list) else 0
        for rows in payload.values()
    )
    _log_runtime_sqlite_timing("save", started_at, target, records=record_count)
    return target


def _load_runtime_store_sqlite(path: str | Path) -> RuntimeStore:
    source = Path(path)
    started_at = perf_counter()
    payload: dict[str, object] = {
        "format_version": RUNTIME_STORE_FORMAT_VERSION,
        **{key: [] for key in _RUNTIME_STORE_COLLECTION_KEYS},
    }
    with sqlite3.connect(str(source), timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_sqlite_runtime_ddl())
        version_row = conn.execute(
            "SELECT value FROM runtime_meta WHERE key = ?",
            ("format_version",),
        ).fetchone()
        if version_row is not None:
            payload["format_version"] = int(version_row["value"])
        for collection in _RUNTIME_STORE_COLLECTION_KEYS:
            table_name = _SQLITE_RUNTIME_TABLES[collection]
            rows = conn.execute(
                f"SELECT rowid, item_key, payload FROM {table_name} WHERE collection = ? ORDER BY rowid",
                (collection,),
            ).fetchall()
            decoded_rows: list[dict[str, object]] = []
            for row in rows:
                try:
                    decoded = _decode_sqlite_runtime_payload(collection, source, str(row["payload"]))
                    _validate_sqlite_runtime_row(collection, decoded)
                    decoded_rows.append(decoded)
                except Exception as exc:
                    _quarantine_sqlite_runtime_row(
                        conn,
                        checkpoint_path=source,
                        table_name=table_name,
                        rowid=int(row["rowid"]),
                        collection=collection,
                        item_key=str(row["item_key"]),
                        payload=str(row["payload"]),
                        reason=str(exc) or exc.__class__.__name__,
                    )
            payload[collection] = decoded_rows
    store = _runtime_store_from_payload(payload)
    record_count = sum(
        len(rows) if isinstance(rows, list) else 0
        for rows in payload.values()
    )
    _log_runtime_sqlite_timing("load", started_at, source, records=record_count)
    return store



def build_runtime_store_payload(store: RuntimeStore) -> dict[str, object]:
    return {
        "format_version": RUNTIME_STORE_FORMAT_VERSION,
        "capsules": [_serialize_capsule(c) for c in store.capsules.values()],
        "events": [_serialize_event(e) for e in store.list_events()],
        "audit_records": [_serialize_audit(r) for r in store.list_audit_records()],
        "progress_updates": [_serialize_progress(p) for p in store.list_progress_updates()],
        "scheduled_tasks": [_serialize_scheduled_task(t) for t in store.list_scheduled_tasks()],
        "reminders": [_serialize_reminder(r) for r in store.list_reminders()],
        "suspended_turns": [_serialize_suspended_turn(turn) for turn in store.list_suspended_turns()],
        "doctor_signals": [_serialize_signal(r) for r in store.list_doctor_signals()],
        "sentinel_signals": [_serialize_signal(s) for s in store.list_sentinel_signals()],
        "sentinel_escalations": [_serialize_escalation(e) for e in store.list_sentinel_escalations()],
        "approval_requests": [_serialize_approval(approval) for approval in store.list_approval_requests()],
        "permission_grants": [_serialize_permission_grant(g) for g in store.list_permission_grants()],
        "boundary_permits": [_serialize_boundary_permit(permit) for permit in store.list_boundary_permits()],
        "boundary_policy_rules": [_serialize_boundary_policy_rule(rule) for rule in store.list_boundary_policy_rules()],
        "doctor_recommendations": store.list_doctor_recommendations(),
        "doctor_actions": store.list_doctor_actions(),
        "mini_agent_runs": [_serialize_mini_agent_run(r) for r in store.list_mini_agent_runs()],
        "missions": [_serialize_mission(mission) for mission in store.list_missions()],
        "builder_proposals": [_serialize_builder_proposal_record(r) for r in store.list_builder_proposals()],
        "skills": [_serialize_skill(skill) for skill in store.skills.values()],
        "user_facts": [_serialize_user_memory_entry(entry) for entry in store.user_facts.values()],
        "preferences": [_serialize_user_memory_entry(entry) for entry in store.preferences.values()],
        "environment_facts": [_serialize_user_memory_entry(entry) for entry in store.environment_facts.values()],
        "skill_execution_plans": [
            _serialize_skill_execution_plan(capsule_id, plan)
            for capsule_id, plan in store.list_skill_execution_plans().items()
        ],
        "task_frames": [
            _serialize_task_frame(frame)
            for frame in store.task_frames.values()
        ],
        "active_task_frames": [
            _serialize_active_task_frame(conversation_id, frame_id)
            for conversation_id, frame_id in store.active_task_frames.items()
        ],
        "conversation_turns": [
            _serialize_conversation_turn(turn)
            for turn in store.conversation_turns.values()
        ],
        "conversation_branches": [
            _serialize_conversation_branch(branch)
            for branch in store.conversation_branches.values()
        ],
        "conversation_heads": [
            _serialize_conversation_head(conversation_id, head)
            for conversation_id, head in store.conversation_heads.items()
        ],
        "conversation_commits": [
            _serialize_conversation_commit(conversation_id, keys)
            for conversation_id, keys in store.conversation_commits.items()
        ],
        "conversation_ingress_ids": [
            _serialize_conversation_ingress_ids(conversation_id, ingress_ids)
            for conversation_id, ingress_ids in store.conversation_ingress_ids.items()
        ],
        "conversation_events": store.list_conversation_events(),
    }



def render_runtime_store_payload_json(store: RuntimeStore) -> str:
    return json.dumps(build_runtime_store_payload(store), indent=2, sort_keys=True)



def save_runtime_store(store: RuntimeStore, path: str | Path) -> Path:
    target = Path(path)
    if _is_sqlite_runtime_path(target):
        return _save_runtime_store_sqlite(store, target)
    with _runtime_store_file_lock(target):
        previous_payload = target.read_text(encoding="utf-8") if target.exists() else None
        if previous_payload is not None:
            try:
                previous_store = _runtime_store_from_payload(json.loads(previous_payload))
                _merge_previous_checkpoint_records(store, previous_store)
            except Exception:
                pass
        payload_json = render_runtime_store_payload_json(store)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_target = target.with_name(
            f"{target.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            temp_target.write_text(payload_json, encoding="utf-8")
            if previous_payload is not None:
                _rotate_runtime_store_backups(target)
                _backup_path_for(target, 0).write_text(previous_payload, encoding="utf-8")
            temp_target.replace(target)
        except Exception:
            if temp_target.exists():
                temp_target.unlink()
            raise
    return target



def _runtime_store_from_payload(payload: dict[str, object]) -> RuntimeStore:
    payload = migrate_runtime_store_payload(payload)
    format_version = int(payload.get("format_version", RUNTIME_STORE_FORMAT_VERSION))
    if format_version not in SUPPORTED_RUNTIME_STORE_FORMAT_VERSIONS:
        raise ValueError(f"Unsupported runtime store format version: {format_version}")

    store = RuntimeStore()
    for capsule in payload.get("capsules", []):
        store.add_capsule(_deserialize_capsule(capsule))
    for event in payload.get("events", []):
        store.add_event(_deserialize_event(event))
    for record in payload.get("audit_records", []):
        store.add_audit_record(_deserialize_audit(record))
    for update in payload.get("progress_updates", []):
        store.add_progress_update(_deserialize_progress(update))
    for task in payload.get("scheduled_tasks", []):
        store.add_scheduled_task(_deserialize_scheduled_task(task))
    for reminder in payload.get("reminders", []):
        store.add_reminder(_deserialize_reminder(reminder))
    for suspended_turn in payload.get("suspended_turns", []):
        store.add_suspended_turn(_deserialize_suspended_turn(suspended_turn))
    for route in payload.get("doctor_signals", []):
        store.add_doctor_signal(_deserialize_signal(route))
    for route in payload.get("sentinel_signals", []):
        store.add_sentinel_signal(_deserialize_signal(route))
    for escalation in payload.get("sentinel_escalations", []):
        store.add_sentinel_escalation(_deserialize_escalation(escalation))
    for approval in payload.get("approval_requests", []):
        store.add_approval_request(_deserialize_approval(approval))
    for grant in payload.get("permission_grants", []):
        store.add_permission_grant(_deserialize_permission_grant(grant))
    for permit in payload.get("boundary_permits", []):
        store.add_boundary_permit(_deserialize_boundary_permit(permit))
    for rule in payload.get("boundary_policy_rules", []):
        store.add_boundary_policy_rule(_deserialize_boundary_policy_rule(rule))
    for recommendation in payload.get("doctor_recommendations", []):
        store.add_doctor_recommendation(recommendation)
    for action in payload.get("doctor_actions", []):
        store.add_doctor_action(action)
    for run in payload.get("mini_agent_runs", []):
        store.add_mini_agent_run(_deserialize_mini_agent_run(run))
    for mission in payload.get("missions", []):
        store.add_mission(_deserialize_mission(dict(mission)))
    for proposal in payload.get("builder_proposals", []):
        record = _deserialize_builder_proposal_record(proposal)
        store.builder_proposals[record.proposal_id] = record
    for skill in payload.get("skills", []):
        skill_record = _deserialize_skill(skill)
        store.skills[skill_record.skill_id] = skill_record
    for user_memory_entry in payload.get("user_facts", []):
        store.add_user_memory_entry(_deserialize_user_memory_entry(dict(user_memory_entry)))
    for user_memory_entry in payload.get("preferences", []):
        store.add_user_memory_entry(_deserialize_user_memory_entry(dict(user_memory_entry)))
    for user_memory_entry in payload.get("environment_facts", []):
        store.add_user_memory_entry(_deserialize_user_memory_entry(dict(user_memory_entry)))
    for skill_execution_plan in payload.get("skill_execution_plans", []):
        capsule_id, plan = _deserialize_skill_execution_plan(skill_execution_plan)
        store.set_skill_execution_plan(capsule_id, plan)
    for task_frame in payload.get("task_frames", []):
        store.add_task_frame(_deserialize_task_frame(dict(task_frame)))
    for active_task_frame in payload.get("active_task_frames", []):
        conversation_id, frame_id = _deserialize_active_task_frame(dict(active_task_frame))
        store.set_active_task_frame_id(conversation_id, frame_id)
    for turn in payload.get("conversation_turns", []):
        store.add_conversation_turn(_deserialize_conversation_turn(dict(turn)))
    for branch in payload.get("conversation_branches", []):
        store.add_conversation_branch(_deserialize_conversation_branch(dict(branch)))
    for head in payload.get("conversation_heads", []):
        conversation_id, active_branch_id, active_turn_id = _deserialize_conversation_head(dict(head))
        store.set_conversation_head(
            conversation_id,
            active_branch_id=active_branch_id,
            active_turn_id=active_turn_id,
        )
    for commit in payload.get("conversation_commits", []):
        conversation_id, idempotency_keys = _deserialize_conversation_commit(dict(commit))
        for idempotency_key in idempotency_keys:
            store.add_committed_idempotency_key(conversation_id, idempotency_key)
    for ingress_record in payload.get("conversation_ingress_ids", []):
        conversation_id, ingress_ids = _deserialize_conversation_ingress_ids(dict(ingress_record))
        for ingress_id in ingress_ids:
            store.add_conversation_ingress_id(conversation_id, ingress_id)
    for conversation_event in payload.get("conversation_events", []):
        store.add_conversation_event(dict(conversation_event))
    return store


def load_runtime_store(path: str | Path) -> RuntimeStore:
    source = Path(path)
    if _is_sqlite_runtime_path(source):
        migrate_sqlite_runtime_memory_payloads(source)
        return _load_runtime_store_sqlite(source)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid runtime store JSON") from exc
    return _runtime_store_from_payload(payload)



def list_runtime_store_backups(path: str | Path) -> list[dict[str, object]]:
    checkpoint_path = Path(path)
    backups: list[dict[str, object]] = []
    generation = 0
    while True:
        candidate = checkpoint_path.with_name(
            f"{checkpoint_path.name}.bak" if generation == 0 else f"{checkpoint_path.name}.bak.{generation}"
        )
        if not candidate.exists():
            break
        backups.append(
            _runtime_backup_record(
                checkpoint_path,
                candidate,
                restore_id=str(generation),
                generation=generation,
                kind="backup",
            )
        )
        generation += 1
    for candidate in sorted(
        checkpoint_path.parent.glob(f"{checkpoint_path.name}.corrupt-*"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
        reverse=True,
    ):
        backups.append(
            _runtime_backup_record(
                checkpoint_path,
                candidate,
                restore_id=candidate.name.removeprefix(f"{checkpoint_path.name}."),
                generation=None,
                kind="recovered",
            )
        )
    for candidate in sorted(
        checkpoint_path.parent.glob(f"{checkpoint_path.name}.pre-smart-restore-*"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
        reverse=True,
    ):
        backups.append(
            _runtime_backup_record(
                checkpoint_path,
                candidate,
                restore_id=candidate.name.removeprefix(f"{checkpoint_path.name}."),
                generation=None,
                kind="manual-snapshot",
            )
        )
    return backups



def get_latest_runtime_restore_metadata(store: RuntimeStore) -> dict[str, object] | None:
    for event in reversed(store.list_events()):
        if event.event_type != "runtime.store_restored":
            continue
        payload = dict(event.payload)
        return {
            "checkpoint_path": payload.get("checkpoint_path"),
            "source": payload.get("source"),
            "generation": payload.get("generation"),
        }
    return None



def restore_runtime_store_backup(path: str | Path, *, generation: int | str = 0) -> Path:
    target = Path(path)
    backup_target = _restore_candidate_for_token(target, generation)
    if not backup_target.exists():
        raise FileNotFoundError(backup_target)
    integrity = _sqlite_quick_check(backup_target)
    if integrity not in {None, "ok"}:
        raise ValueError(f"Runtime backup failed integrity check: {backup_target.name} ({integrity})")

    temp_target = target.with_name(f"{target.name}.restore.tmp")
    try:
        temp_target.write_bytes(backup_target.read_bytes())
        temp_target.replace(target)
    except Exception:
        if temp_target.exists():
            temp_target.unlink()
        raise
    return target


def build_runtime_from_settings(settings: object) -> "PersistentRuntime":
    """Construct a PersistentRuntime from a Settings object.

    Reads ``settings.checkpoint_path`` (falls back to
    ``~/.nullion/runtime.db``) and bootstraps the store.
    """
    from pathlib import Path as _Path
    from nullion.runtime import bootstrap_persistent_runtime

    checkpoint = getattr(settings, "checkpoint_path", None)
    if checkpoint is None:
        checkpoint = _Path.home() / ".nullion" / "runtime.db"
    return bootstrap_persistent_runtime(_Path(checkpoint))


__all__ = [
    "save_runtime_store",
    "load_runtime_store",
    "migrate_runtime_store_payload",
    "migrate_sqlite_runtime_memory_payloads",
    "list_runtime_store_backups",
    "get_latest_runtime_restore_metadata",
    "restore_runtime_store_backup",
    "build_runtime_from_settings",
]
