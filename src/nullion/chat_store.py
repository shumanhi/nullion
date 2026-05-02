"""Persistent chat history for the Nullion web UI.

SQLite database at ~/.nullion/chat_history.db.

Schema
------
conversations
    id              TEXT PRIMARY KEY   -- matches the web conversation_id
    created_at      TEXT NOT NULL      -- ISO-8601
    last_message_at TEXT               -- ISO-8601, updated on each new message
    title           TEXT               -- auto-set from first user message (truncated)
    status          TEXT DEFAULT 'active'  -- 'active' | 'archived' | 'cleared'
    channel         TEXT NOT NULL DEFAULT 'web'   -- machine ID e.g. 'web', 'telegram:123456789'
    channel_label   TEXT NOT NULL DEFAULT 'Web'   -- human display name e.g. 'Web', 'Telegram · Himan'

messages
    id              INTEGER PRIMARY KEY AUTOINCREMENT
    conversation_id TEXT NOT NULL REFERENCES conversations(id)
    role            TEXT NOT NULL      -- 'user' | 'bot'
    text            TEXT NOT NULL
    metadata        TEXT               -- encrypted JSON for attachments/artifacts
    is_error        INTEGER DEFAULT 0  -- 1 if this is an error bubble
    created_at      TEXT NOT NULL      -- ISO-8601

Usage
-----
    from nullion.chat_store import ChatStore
    store = ChatStore()
    store.save_message(conv_id, 'user', 'Hello!')
    msgs = store.load_messages(conv_id)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from cryptography.fernet import Fernet, InvalidToken

from nullion.redaction import redact_text
from nullion.secure_storage import (
    KEYCHAIN_ACCOUNT,
    KEYCHAIN_SERVICE,
    _keychain_get,
    configured_key_storage,
    load_or_create_fernet_key,
)

log = logging.getLogger(__name__)

_DB_PATH = Path.home() / ".nullion" / "chat_history.db"
_KEY_PATH = Path.home() / ".nullion" / "chat_history.key"
_ENCRYPTED_PREFIX = "enc:v1:"

_DDL = """
CREATE TABLE IF NOT EXISTS conversations (
    id              TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    last_message_at TEXT,
    title           TEXT,
    status          TEXT NOT NULL DEFAULT 'active',
    channel         TEXT NOT NULL DEFAULT 'web',
    channel_label   TEXT NOT NULL DEFAULT 'Web'
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    role            TEXT NOT NULL,
    text            TEXT NOT NULL,
    metadata        TEXT,
    is_error        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conv
    ON messages (conversation_id, created_at);
CREATE INDEX IF NOT EXISTS idx_conversations_status
    ON conversations (status, last_message_at);
"""

_POST_MIGRATION_DDL = """
CREATE INDEX IF NOT EXISTS idx_conversations_channel
    ON conversations (channel, last_message_at);
"""

_MAX_TITLE_LEN = 60

# Columns added via migration (not present in older DBs)
_CONVERSATION_MIGRATION_COLUMNS = [
    ("channel", "TEXT NOT NULL DEFAULT 'web'"),
    ("channel_label", "TEXT NOT NULL DEFAULT 'Web'"),
]
_MESSAGE_MIGRATION_COLUMNS = [
    ("metadata", "TEXT"),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _make_title(text: str) -> str:
    text = text.strip()
    if len(text) <= _MAX_TITLE_LEN:
        return text
    return text[:_MAX_TITLE_LEN].rsplit(" ", 1)[0] + "…"


def _channel_label_for_conversation(conversation_id: str) -> str:
    if conversation_id == "web" or conversation_id.startswith("web:"):
        return "Web"
    if conversation_id.startswith("telegram:"):
        return f"Telegram · {conversation_id.removeprefix('telegram:')}"
    if conversation_id.startswith("slack:"):
        return f"Slack · {conversation_id.removeprefix('slack:')}"
    if conversation_id.startswith("discord:"):
        return f"Discord · {conversation_id.removeprefix('discord:')}"
    return conversation_id


class ChatStore:
    """Thread-safe SQLite-backed chat history store.

    A single instance is safe to share across threads — each operation
    acquires a reentrant lock and opens its own short-lived connection.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        key_path: Path | str | None = None,
    ) -> None:
        self._path = Path(db_path) if db_path else _DB_PATH
        self._key_path = Path(key_path) if key_path else _KEY_PATH
        primary_key = self._load_or_create_key()
        self._cipher = Fernet(primary_key)
        self._decrypt_ciphers = [self._cipher]
        for key in self._alternate_decryption_keys(primary_key):
            try:
                self._decrypt_ciphers.append(Fernet(key))
            except Exception:
                log.warning("Ignoring invalid alternate chat-history key.")
        self._lock = threading.RLock()
        self._init_db()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load_or_create_key(self) -> bytes:
        """Load the configured chat-history encryption key.

        The SQLite database stores encrypted message/title fields. This key stays
        outside SQLite so a copied database is not directly readable.
        """
        return load_or_create_fernet_key(self._key_path)

    def _alternate_decryption_keys(self, primary_key: bytes) -> list[bytes]:
        """Return legacy keys that may decrypt older chat rows.

        Some early installs wrote chat rows with the local key while later runs
        used Keychain, or vice versa. New rows use the configured primary key,
        but reads should tolerate either key so history does not turn into
        placeholder text after a storage migration.
        """
        candidates: list[bytes] = []
        if self._key_path.exists():
            try:
                os.chmod(self._key_path, 0o600)
                candidates.append(self._key_path.read_bytes().strip())
            except OSError:
                log.warning("Could not read local chat-history key fallback: %s", self._key_path)
        if configured_key_storage() != "keychain":
            try:
                keychain_value = _keychain_get(service=KEYCHAIN_SERVICE, account=KEYCHAIN_ACCOUNT)
                if keychain_value:
                    candidates.append(keychain_value.encode("ascii"))
            except Exception:
                log.debug("Could not read Keychain chat-history fallback.", exc_info=True)
        unique: list[bytes] = []
        seen = {primary_key}
        for key in candidates:
            if key and key not in seen:
                seen.add(key)
                unique.append(key)
        return unique

    def _encrypt_text(self, text: str | None) -> str | None:
        if text is None:
            return None
        if text.startswith(_ENCRYPTED_PREFIX):
            return text
        token = self._cipher.encrypt(text.encode("utf-8")).decode("ascii")
        return _ENCRYPTED_PREFIX + token

    def _decrypt_text(self, text: str | None) -> str | None:
        if text is None:
            return None
        if not text.startswith(_ENCRYPTED_PREFIX):
            return text
        token = text.removeprefix(_ENCRYPTED_PREFIX).encode("ascii")
        for cipher in self._decrypt_ciphers:
            try:
                return cipher.decrypt(token).decode("utf-8")
            except InvalidToken:
                continue
        log.error("Could not decrypt chat history field; key may not match the database.")
        return "[encrypted: unreadable]"

    def _decrypt_row(self, row: sqlite3.Row | dict) -> dict:
        data = dict(row)
        if "title" in data:
            data["title"] = self._decrypt_text(data.get("title"))
        if "text" in data:
            data["text"] = self._decrypt_text(data.get("text")) or ""
        if "metadata" in data:
            raw_metadata = self._decrypt_text(data.get("metadata"))
            try:
                data["metadata"] = json.loads(raw_metadata) if raw_metadata else {}
            except json.JSONDecodeError:
                data["metadata"] = {}
        return data

    def _init_db(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_DDL)
            # Migrate: add channel / channel_label columns if missing
            existing_columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(conversations)").fetchall()
            }
            for col_name, col_def in _CONVERSATION_MIGRATION_COLUMNS:
                if col_name not in existing_columns:
                    conn.execute(
                        f"ALTER TABLE conversations ADD COLUMN {col_name} {col_def}"
                    )
                    log.info("Migrated conversations: added column %s", col_name)
            existing_message_columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(messages)").fetchall()
            }
            for col_name, col_def in _MESSAGE_MIGRATION_COLUMNS:
                if col_name not in existing_message_columns:
                    conn.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_def}")
                    log.info("Migrated messages: added column %s", col_name)
            conn.executescript(_POST_MIGRATION_DDL)
            self._encrypt_existing_plaintext(conn)

    def _encrypt_existing_plaintext(self, conn: sqlite3.Connection) -> None:
        """Encrypt legacy plaintext message/title fields in-place."""
        message_rows = conn.execute(
            "SELECT id, text FROM messages WHERE text NOT LIKE ?",
            (_ENCRYPTED_PREFIX + "%",),
        ).fetchall()
        for row in message_rows:
            conn.execute(
                "UPDATE messages SET text = ? WHERE id = ?",
                (self._encrypt_text(row["text"]), row["id"]),
            )

        existing_message_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(messages)").fetchall()
        }
        if "metadata" in existing_message_columns:
            metadata_rows = conn.execute(
                "SELECT id, metadata FROM messages WHERE metadata IS NOT NULL AND metadata NOT LIKE ?",
                (_ENCRYPTED_PREFIX + "%",),
            ).fetchall()
            for row in metadata_rows:
                conn.execute(
                    "UPDATE messages SET metadata = ? WHERE id = ?",
                    (self._encrypt_text(row["metadata"]), row["id"]),
                )

        title_rows = conn.execute(
            "SELECT id, title FROM conversations WHERE title IS NOT NULL AND title NOT LIKE ?",
            (_ENCRYPTED_PREFIX + "%",),
        ).fetchall()
        for row in title_rows:
            conn.execute(
                "UPDATE conversations SET title = ? WHERE id = ?",
                (self._encrypt_text(row["title"]), row["id"]),
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = sqlite3.connect(str(self._path), timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ── Conversation management ───────────────────────────────────────────────

    def ensure_conversation(
        self,
        conv_id: str,
        *,
        channel: str = "web",
        channel_label: str = "Web",
    ) -> None:
        """Create a conversation record if it doesn't exist yet."""
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO conversations
                   (id, created_at, channel, channel_label)
                   VALUES (?, ?, ?, ?)""",
                (conv_id, _now(), channel, channel_label),
            )

    def get_conversation(self, conv_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM conversations WHERE id = ?", (conv_id,)
            ).fetchone()
            return self._decrypt_row(row) if row else None

    def list_conversations(
        self,
        status: str = "archived",
        limit: int = 100,
        *,
        channel: str | None = None,
    ) -> list[dict]:
        """Return conversations with the given status, newest first.

        Optionally filter by ``channel`` (e.g. ``'web'`` or ``'telegram:123'``).
        """
        with self._connect() as conn:
            if channel is None:
                rows = conn.execute(
                    """SELECT c.*, COUNT(m.id) AS message_count, MAX(m.id) AS latest_message_id
                       FROM conversations c
                       LEFT JOIN messages m ON m.conversation_id = c.id
                       WHERE c.status = ?
                       GROUP BY c.id
                       ORDER BY COALESCE(MAX(m.id), 0) DESC, COALESCE(c.last_message_at, c.created_at) DESC
                       LIMIT ?""",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT c.*, COUNT(m.id) AS message_count, MAX(m.id) AS latest_message_id
                       FROM conversations c
                       LEFT JOIN messages m ON m.conversation_id = c.id
                       WHERE c.status = ? AND c.channel = ?
                       GROUP BY c.id
                       ORDER BY COALESCE(MAX(m.id), 0) DESC, COALESCE(c.last_message_at, c.created_at) DESC
                       LIMIT ?""",
                    (status, channel, limit),
                ).fetchall()
            return [self._decrypt_row(r) for r in rows]

    def list_channels(self) -> list[dict]:
        """Return distinct channels with metadata, ordered by most-recently active.

        Each entry contains:
            channel            -- machine ID (e.g. 'web', 'telegram:123456789')
            channel_label      -- human display name (e.g. 'Web', 'Telegram · Himan')
            conversation_count -- number of non-cleared conversations on this channel
            last_message_at    -- ISO-8601 timestamp of the most recent message
        """
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT channel, channel_label,
                          COUNT(DISTINCT c.id) AS conversation_count,
                          MAX(c.last_message_at) AS last_message_at
                   FROM conversations c
                   WHERE c.status != 'cleared'
                   GROUP BY channel
                   ORDER BY last_message_at DESC"""
            ).fetchall()
            return [self._decrypt_row(r) for r in rows]

    def calendar_days(self, channel: str, year_month: str) -> dict[str, int]:
        """Return a map of ``'YYYY-MM-DD'`` → message count for the given channel/month.

        ``year_month`` must be in ``'YYYY-MM'`` format.  The result can be used
        to highlight days on a calendar widget.
        """
        import re as _re
        if not _re.fullmatch(r"\d{4}-\d{2}", year_month):
            raise ValueError(f"year_month must be in YYYY-MM format, got {year_month!r}")
        like_pattern = year_month + "-%"
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT date(m.created_at) AS day, COUNT(m.id) AS msg_count
                   FROM messages m
                   JOIN conversations c ON c.id = m.conversation_id
                   WHERE c.channel = ? AND date(m.created_at) LIKE ?
                   GROUP BY day""",
                (channel, like_pattern),
            ).fetchall()
            return {row["day"]: row["msg_count"] for row in rows}

    def list_conversations_for_channel_date(
        self, channel: str, date: str
    ) -> list[dict]:
        """Return conversations on ``channel`` that have at least one message on ``date``.

        ``date`` must be in ``'YYYY-MM-DD'`` format.  Each entry includes a
        ``message_count`` field reflecting the number of messages on that day.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT c.id, c.title, c.channel, c.channel_label,
                          c.created_at, c.last_message_at,
                          COUNT(m.id) AS message_count
                   FROM conversations c
                   JOIN messages m ON m.conversation_id = c.id
                   WHERE c.channel = ? AND date(m.created_at) = ?
                   GROUP BY c.id
                   ORDER BY MIN(m.created_at) ASC""",
                (channel, date),
            ).fetchall()
            return [self._decrypt_row(r) for r in rows]

    def archive_conversation(self, conv_id: str) -> bool:
        """Mark a conversation as archived. Returns True if it existed."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE conversations SET status = 'archived' WHERE id = ? AND status = 'active'",
                (conv_id,),
            )
            return cur.rowcount > 0

    def clear_conversation(self, conv_id: str) -> bool:
        """Delete all messages and mark conversation as cleared.

        The conversation record is kept so that the conv_id is not reused.
        Returns True if conversation existed.
        """
        with self._connect() as conn:
            conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conv_id,))
            cur = conn.execute(
                "UPDATE conversations SET status = 'cleared', title = NULL WHERE id = ?",
                (conv_id,),
            )
            return cur.rowcount > 0

    def delete_conversation_permanently(self, conv_id: str) -> bool:
        """Hard-delete messages and the conversation record. Irreversible."""
        with self._connect() as conn:
            conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conv_id,))
            cur = conn.execute("DELETE FROM conversations WHERE id = ?", (conv_id,))
            return cur.rowcount > 0

    def delete_conversations_for_channel_date(self, channel: str, date: str) -> int:
        """Hard-delete conversations on ``channel`` with messages on ``date``.

        Returns the number of conversation records removed.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT DISTINCT c.id
                   FROM conversations c
                   JOIN messages m ON m.conversation_id = c.id
                   WHERE c.channel = ? AND date(m.created_at) = ?""",
                (channel, date),
            ).fetchall()
            conv_ids = [row["id"] for row in rows]
            for conv_id in conv_ids:
                conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conv_id,))
                conn.execute("DELETE FROM conversations WHERE id = ?", (conv_id,))
            return len(conv_ids)

    # ── Message management ────────────────────────────────────────────────────

    def save_message(
        self,
        conv_id: str,
        role: str,
        text: str,
        *,
        is_error: bool = False,
        metadata: dict[str, Any] | None = None,
        channel: str = "web",
        channel_label: str = "Web",
    ) -> int:
        """Save a message and return its row id.

        Automatically creates the conversation record if needed, and
        sets the conversation title from the first user message.
        ``channel`` and ``channel_label`` are applied only when the
        conversation is first created.
        """
        if not text or not text.strip():
            return -1
        safe_text = redact_text(text)
        metadata_text = None
        if metadata:
            metadata_text = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
        now = _now()
        with self._connect() as conn:
            # Upsert conversation (channel columns set only on first insert)
            conn.execute(
                """INSERT OR IGNORE INTO conversations
                   (id, created_at, channel, channel_label)
                   VALUES (?, ?, ?, ?)""",
                (conv_id, now, channel, channel_label),
            )
            # Auto-title from first user message
            if role == "user":
                existing_title = conn.execute(
                    "SELECT title FROM conversations WHERE id = ?", (conv_id,)
                ).fetchone()
                if existing_title and not existing_title["title"]:
                    conn.execute(
                        "UPDATE conversations SET title = ? WHERE id = ?",
                        (self._encrypt_text(_make_title(safe_text)), conv_id),
                    )
            # Insert message
            cur = conn.execute(
                "INSERT INTO messages (conversation_id, role, text, metadata, is_error, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    conv_id,
                    role,
                    self._encrypt_text(safe_text),
                    self._encrypt_text(metadata_text) if metadata_text else None,
                    1 if is_error else 0,
                    now,
                ),
            )
            # Update last_message_at
            conn.execute(
                "UPDATE conversations SET last_message_at = ? WHERE id = ?",
                (now, conv_id),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def import_runtime_chat_turns(self, turns: Iterable[dict]) -> int:
        """Import messaging chat turns from the runtime event log.

        Older web/Slack/Discord/Telegram turns may exist in ``runtime-store.json``
        but predate the unified web history database. This importer backfills
        those turns while avoiding obvious duplicates from adapters that already
        persisted directly to ``chat_history.db``.
        """
        imported = 0
        existing_by_conversation: dict[str, set[tuple[str, str]]] = {}
        sorted_turns = sorted(
            (turn for turn in turns if isinstance(turn, dict)),
            key=lambda turn: str(turn.get("created_at") or ""),
        )
        with self._connect() as conn:
            for turn in sorted_turns:
                conversation_id = str(turn.get("conversation_id") or "").strip()
                if not conversation_id.startswith(("web:", "telegram:", "slack:", "discord:")):
                    continue
                created_at = str(turn.get("created_at") or "").strip() or _now()
                user_message = str(turn.get("user_message") or "").strip()
                assistant_reply = str(turn.get("assistant_reply") or "").strip()
                if not user_message and not assistant_reply:
                    continue

                if conversation_id not in existing_by_conversation:
                    rows = conn.execute(
                        """SELECT role, text FROM messages
                           WHERE conversation_id = ?
                           ORDER BY id ASC""",
                        (conversation_id,),
                    ).fetchall()
                    existing_by_conversation[conversation_id] = {
                        (str(row["role"]), self._decrypt_text(row["text"]) or "")
                        for row in rows
                    }

                channel_label = _channel_label_for_conversation(conversation_id)
                conn.execute(
                    """INSERT OR IGNORE INTO conversations
                       (id, created_at, channel, channel_label)
                       VALUES (?, ?, ?, ?)""",
                    (conversation_id, created_at, conversation_id, channel_label),
                )

                for role, raw_text in (("user", user_message), ("bot", assistant_reply)):
                    if not raw_text:
                        continue
                    safe_text = redact_text(raw_text)
                    key = (role, safe_text)
                    if key in existing_by_conversation[conversation_id]:
                        continue
                    if role == "user":
                        existing_title = conn.execute(
                            "SELECT title FROM conversations WHERE id = ?", (conversation_id,)
                        ).fetchone()
                        if existing_title and not existing_title["title"]:
                            conn.execute(
                                "UPDATE conversations SET title = ? WHERE id = ?",
                                (self._encrypt_text(_make_title(safe_text)), conversation_id),
                            )
                    conn.execute(
                        "INSERT INTO messages (conversation_id, role, text, is_error, created_at) "
                        "VALUES (?, ?, ?, 0, ?)",
                        (conversation_id, role, self._encrypt_text(safe_text), created_at),
                    )
                    conn.execute(
                        """UPDATE conversations
                           SET last_message_at = CASE
                               WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                               ELSE last_message_at
                           END
                           WHERE id = ?""",
                        (created_at, created_at, conversation_id),
                    )
                    existing_by_conversation[conversation_id].add(key)
                    imported += 1
        return imported

    def load_messages(self, conv_id: str, limit: int = 300) -> list[dict]:
        """Return the last ``limit`` messages in ascending chronological order."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT id, role, text, metadata, is_error, created_at
                   FROM (
                       SELECT * FROM messages
                       WHERE conversation_id = ?
                       ORDER BY id DESC
                       LIMIT ?
                   ) sub
                   ORDER BY id ASC""",
                (conv_id, limit),
            ).fetchall()
            return [self._decrypt_row(r) for r in rows]

    def message_count(self, conv_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?", (conv_id,)
            ).fetchone()
            return row[0] if row else 0


# ── Module-level singleton ────────────────────────────────────────────────────

_store: ChatStore | None = None
_store_lock = threading.Lock()


def get_chat_store() -> ChatStore:
    """Return (and lazily initialise) the module-level ChatStore singleton."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                try:
                    _store = ChatStore()
                except Exception as exc:
                    log.error("Failed to initialise ChatStore: %s", exc)
                    raise
    return _store
