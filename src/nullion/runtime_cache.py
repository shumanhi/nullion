"""Shared hot and persistent cache helpers for Nullion runtime work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any

from nullion.latency_tracing import latency_span


logger = logging.getLogger(__name__)

_CACHE_TABLE = "runtime_cache"
_HOT_CACHE_MAX_ENTRIES = 256
_HOT_CACHE: dict[str, dict[str, Any]] = {}
_HOT_CACHE_LOCK = threading.RLock()


@dataclass(frozen=True, slots=True)
class RuntimeCacheResult:
    value: Any | None
    hit: bool
    source: str = "miss"
    elapsed_ms: float = 0.0


def _now() -> datetime:
    return datetime.now(UTC)


def _runtime_db_path(db_path: str | Path | None = None) -> Path | None:
    if db_path is not None:
        candidate = Path(db_path).expanduser()
    else:
        raw = os.environ.get("NULLION_CHECKPOINT_PATH")
        if raw:
            candidate = Path(raw).expanduser()
        else:
            home = Path(str(os.environ.get("NULLION_HOME") or Path.home() / ".nullion")).expanduser()
            candidate = home / "runtime.db"
    if candidate.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
        candidate = candidate.with_name("runtime.db")
    return candidate


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_json_safe(item) for item in value), key=repr)
    if hasattr(value, "value"):
        return str(getattr(value, "value"))
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def cache_key(namespace: str, key_parts: Any, *, version: str = "v1", signature: Any | None = None) -> str:
    payload = {
        "namespace": str(namespace),
        "version": str(version),
        "key": _json_safe(key_parts),
        "signature": _json_safe(signature),
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _hot_get(cache_id: str, *, ttl_seconds: int | None) -> RuntimeCacheResult | None:
    row = _HOT_CACHE.get(cache_id)
    if not row:
        return None
    created_at = float(row.get("created_monotonic", 0.0) or 0.0)
    if ttl_seconds and ttl_seconds > 0 and created_at and time.monotonic() - created_at > ttl_seconds:
        _HOT_CACHE.pop(cache_id, None)
        return None
    return RuntimeCacheResult(value=row.get("value"), hit=True, source="hot")


def _hot_set(cache_id: str, value: Any, *, namespace: str) -> None:
    with _HOT_CACHE_LOCK:
        if len(_HOT_CACHE) >= _HOT_CACHE_MAX_ENTRIES:
            oldest_key = min(_HOT_CACHE, key=lambda item: float(_HOT_CACHE[item].get("accessed_monotonic", 0.0) or 0.0))
            _HOT_CACHE.pop(oldest_key, None)
        now_monotonic = time.monotonic()
        _HOT_CACHE[cache_id] = {
            "namespace": namespace,
            "value": value,
            "created_monotonic": now_monotonic,
            "accessed_monotonic": now_monotonic,
        }


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_CACHE_TABLE} (
            cache_id TEXT PRIMARY KEY,
            namespace TEXT NOT NULL,
            version TEXT NOT NULL,
            key_json TEXT NOT NULL,
            signature_json TEXT,
            value_json TEXT NOT NULL,
            persistent INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            accessed_at TEXT NOT NULL,
            expires_at TEXT,
            hit_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_runtime_cache_namespace ON {_CACHE_TABLE} (namespace)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_runtime_cache_expires ON {_CACHE_TABLE} (expires_at)")


def _prune_namespace(conn: sqlite3.Connection, namespace: str, *, max_entries: int | None) -> None:
    if not max_entries or max_entries <= 0:
        return
    rows = conn.execute(
        f"""
        SELECT cache_id FROM {_CACHE_TABLE}
        WHERE namespace = ?
        ORDER BY accessed_at DESC, updated_at DESC
        LIMIT -1 OFFSET ?
        """,
        (namespace, int(max_entries)),
    ).fetchall()
    stale = [(str(row[0]),) for row in rows]
    if stale:
        conn.executemany(f"DELETE FROM {_CACHE_TABLE} WHERE cache_id = ?", stale)


def get_json(
    namespace: str,
    key_parts: Any,
    *,
    version: str = "v1",
    signature: Any | None = None,
    ttl_seconds: int | None = None,
    persistent: bool = True,
    db_path: str | Path | None = None,
) -> RuntimeCacheResult:
    with latency_span("nullion.cache.get", namespace=namespace, persistent=persistent):
        started = time.perf_counter()
        cache_id = cache_key(namespace, key_parts, version=version, signature=signature)
        with _HOT_CACHE_LOCK:
            hot = _hot_get(cache_id, ttl_seconds=ttl_seconds)
            if hot is not None:
                return RuntimeCacheResult(hot.value, True, hot.source, (time.perf_counter() - started) * 1000)
        if not persistent:
            return RuntimeCacheResult(None, False, "miss", (time.perf_counter() - started) * 1000)
        target = _runtime_db_path(db_path)
        if target is None:
            return RuntimeCacheResult(None, False, "miss", (time.perf_counter() - started) * 1000)
        try:
            with sqlite3.connect(str(target), timeout=1) as conn:
                _ensure_table(conn)
                row = conn.execute(
                    f"SELECT value_json, expires_at FROM {_CACHE_TABLE} WHERE cache_id = ?",
                    (cache_id,),
                ).fetchone()
                if row is None:
                    return RuntimeCacheResult(None, False, "miss", (time.perf_counter() - started) * 1000)
                expires_at = row[1]
                if expires_at:
                    try:
                        expires = datetime.fromisoformat(str(expires_at))
                        if expires.tzinfo is None:
                            expires = expires.replace(tzinfo=UTC)
                        if expires <= _now():
                            conn.execute(f"DELETE FROM {_CACHE_TABLE} WHERE cache_id = ?", (cache_id,))
                            return RuntimeCacheResult(None, False, "expired", (time.perf_counter() - started) * 1000)
                    except ValueError:
                        conn.execute(f"DELETE FROM {_CACHE_TABLE} WHERE cache_id = ?", (cache_id,))
                        return RuntimeCacheResult(None, False, "invalid", (time.perf_counter() - started) * 1000)
                value = json.loads(str(row[0]))
                conn.execute(
                    f"UPDATE {_CACHE_TABLE} SET accessed_at = ?, hit_count = hit_count + 1 WHERE cache_id = ?",
                    (_now().isoformat(), cache_id),
                )
                _hot_set(cache_id, value, namespace=namespace)
                return RuntimeCacheResult(value, True, "persistent", (time.perf_counter() - started) * 1000)
        except Exception:
            logger.debug("runtime cache get failed namespace=%s", namespace, exc_info=True)
            return RuntimeCacheResult(None, False, "error", (time.perf_counter() - started) * 1000)


def set_json(
    namespace: str,
    key_parts: Any,
    value: Any,
    *,
    version: str = "v1",
    signature: Any | None = None,
    ttl_seconds: int | None = None,
    persistent: bool = True,
    db_path: str | Path | None = None,
    max_entries: int | None = None,
) -> None:
    with latency_span("nullion.cache.set", namespace=namespace, persistent=persistent):
        cache_id = cache_key(namespace, key_parts, version=version, signature=signature)
        safe_value = _json_safe(value)
        _hot_set(cache_id, safe_value, namespace=namespace)
        if not persistent:
            return
        target = _runtime_db_path(db_path)
        if target is None:
            return
        now = _now()
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat() if ttl_seconds and ttl_seconds > 0 else None
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(target), timeout=1) as conn:
                _ensure_table(conn)
                conn.execute(
                f"""
                INSERT OR REPLACE INTO {_CACHE_TABLE}
                    (cache_id, namespace, version, key_json, signature_json, value_json,
                     persistent, created_at, updated_at, accessed_at, expires_at, hit_count)
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    1,
                    COALESCE((SELECT created_at FROM {_CACHE_TABLE} WHERE cache_id = ?), ?),
                    ?,
                    ?,
                    ?,
                    COALESCE((SELECT hit_count FROM {_CACHE_TABLE} WHERE cache_id = ?), 0)
                )
                """,
                (
                    cache_id,
                    namespace,
                    version,
                    _json_dumps(key_parts),
                    _json_dumps(signature) if signature is not None else None,
                    _json_dumps(safe_value),
                    cache_id,
                    now.isoformat(),
                    now.isoformat(),
                    now.isoformat(),
                    expires_at,
                    cache_id,
                ),
            )
                _prune_namespace(conn, namespace, max_entries=max_entries)
        except Exception:
            logger.debug("runtime cache set failed namespace=%s", namespace, exc_info=True)


def invalidate_namespace(namespace: str, *, db_path: str | Path | None = None) -> None:
    with _HOT_CACHE_LOCK:
        for cache_id, row in list(_HOT_CACHE.items()):
            if row.get("namespace") == namespace:
                _HOT_CACHE.pop(cache_id, None)
    target = _runtime_db_path(db_path)
    if target is None or not target.exists():
        return
    try:
        with sqlite3.connect(str(target), timeout=1) as conn:
            _ensure_table(conn)
            conn.execute(f"DELETE FROM {_CACHE_TABLE} WHERE namespace = ?", (namespace,))
    except Exception:
        logger.debug("runtime cache namespace invalidation failed namespace=%s", namespace, exc_info=True)


def invalidate_prefix(prefix: str, *, db_path: str | Path | None = None) -> None:
    with _HOT_CACHE_LOCK:
        for cache_id, row in list(_HOT_CACHE.items()):
            namespace = str(row.get("namespace") or "")
            if namespace == prefix or namespace.startswith(prefix + "."):
                _HOT_CACHE.pop(cache_id, None)
    target = _runtime_db_path(db_path)
    if target is None or not target.exists():
        return
    try:
        with sqlite3.connect(str(target), timeout=1) as conn:
            _ensure_table(conn)
            conn.execute(
                f"DELETE FROM {_CACHE_TABLE} WHERE namespace = ? OR namespace LIKE ?",
                (prefix, f"{prefix}.%"),
            )
    except Exception:
        logger.debug("runtime cache prefix invalidation failed prefix=%s", prefix, exc_info=True)


def clear_hot_cache() -> None:
    with _HOT_CACHE_LOCK:
        _HOT_CACHE.clear()
