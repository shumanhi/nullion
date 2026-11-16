"""Release checks and durable, once-per-release operator notices.

Adapters own their transport; this module owns content, caching and claims.
Checks run off the event loop, never as part of an ordinary chat turn.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
import subprocess
import time
from uuid import uuid4

from nullion import updater
from nullion.chat_text import UserMessage, render_user_message
from nullion.delivery_acknowledgement import send_error_replay_safety
from nullion.sqlite_connections import sqlite_connection

logger = logging.getLogger(__name__)
CHECK_INTERVAL = 3600
POLL_INTERVAL = 60


@dataclass(frozen=True)
class UpdateAvailability:
    current: str = 'unknown'
    latest: str = 'unknown'
    has_update: bool = False
    release: str = ''
    commit: str = ''


def _database(runtime) -> Path:
    path = Path(runtime.checkpoint_path)
    # Legacy JSON runtimes still need durable claims, never process-local sets.
    if path.suffix.lower() not in {'.db', '.sqlite', '.sqlite3'}:
        path = path.with_suffix('.updates.sqlite3')
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect(runtime):
    return sqlite_connection(str(_database(runtime)), timeout=10)


def _schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS update_release_checks (
        source TEXT PRIMARY KEY, current TEXT NOT NULL, result TEXT NOT NULL,
        expires REAL NOT NULL, owner TEXT NOT NULL, lease_until REAL NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS update_release_notices (
        release TEXT NOT NULL, channel TEXT NOT NULL, target TEXT NOT NULL,
        status TEXT NOT NULL, updated REAL NOT NULL, retry_after REAL NOT NULL,
        PRIMARY KEY (release, channel, target))''')


def check_available_update(runtime, *, now=None) -> UpdateAvailability:
    """One shared hourly release lookup, with a cross-process lease.

    A candidate ahead of a release must not be advertised as needing a downgrade.
    Failed lookups are quiet and cached briefly. HEAD changes invalidate the cache.
    """
    now = time.time() if now is None else now
    source = updater._src_dir().resolve()
    try:
        code, current = updater._git('rev-parse', '--short', 'HEAD', cwd=source)
    except (OSError, subprocess.TimeoutExpired):
        logger.debug('Installed version is unavailable', exc_info=True)
        return UpdateAvailability()
    if code:
        return UpdateAvailability()
    empty = UpdateAvailability(current=current, latest=current)
    owner = uuid4().hex
    with _connect(runtime) as conn:
        _schema(conn)
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT current,result,expires,lease_until FROM update_release_checks WHERE source=?',
                           (str(source),)).fetchone()
        if row and row[0] == current and row[2] > now:
            return UpdateAvailability(**json.loads(row[1]))
        if row and row[3] > now:
            return empty
        conn.execute('''INSERT INTO update_release_checks VALUES (?,?,?,?,?,?)
            ON CONFLICT(source) DO UPDATE SET current=excluded.current, result=excluded.result,
            expires=excluded.expires, owner=excluded.owner, lease_until=excluded.lease_until''',
            (str(source), current, json.dumps(asdict(empty)), 0, owner, now + 180))
    result, ttl = empty, CHECK_INTERVAL
    try:
        target = updater._update_target('release', cwd=source)
        relation = updater._commit_relation(current, target.commit, cwd=source)
        # Do not publish a stale check if an update happened during the fetch.
        if updater._current_commit(cwd=source) != current:
            ttl = 0
        else:
            result = UpdateAvailability(current=current, latest=target.label,
                has_update=relation == 'behind', release=target.ref, commit=target.commit)
    except Exception:
        ttl = 300
        logger.debug('Release availability check failed', exc_info=True)
    with _connect(runtime) as conn:
        conn.execute('''UPDATE update_release_checks SET result=?, expires=?, lease_until=0
            WHERE source=? AND owner=?''', (json.dumps(asdict(result)), now + ttl, str(source), owner))
    return result


def update_notice_text(availability: UpdateAvailability, *, channel: str) -> str:
    from nullion.operator_commands import operator_command_invocation
    command = operator_command_invocation('/update', platform=channel)
    return render_user_message(UserMessage(summary=f'Nullion {availability.latest} is available.',
        paragraphs=(f'Send `{command}` to update when you’re ready.',), kind='update_available'))


def _claim_notice(runtime, release, channel, target, now):
    with _connect(runtime) as conn:
        _schema(conn)
        conn.execute('BEGIN IMMEDIATE')
        inserted = conn.execute('INSERT OR IGNORE INTO update_release_notices VALUES (?,?,?,?,?,?)',
            (release, channel, target, 'claimed', now, 0)).rowcount
        if inserted:
            return True
        return bool(conn.execute('''UPDATE update_release_notices SET status='claimed',updated=?,retry_after=0
            WHERE release=? AND channel=? AND target=? AND status='retry' AND retry_after<=?''',
            (now, release, channel, target, now)).rowcount)


def _finish_notice(runtime, release, channel, target, status, now):
    with _connect(runtime) as conn:
        conn.execute('''UPDATE update_release_notices SET status=?,updated=?,retry_after=?
            WHERE release=? AND channel=? AND target=?''',
            (status, now, now + 300 if status == 'retry' else 0, release, channel, target))


async def deliver_update_notice(runtime, *, channel, target, send, now=None,
                                replay_safety=send_error_replay_safety) -> bool:
    """Claim before sending. Never replay an accepted or uncertain delivery."""
    if channel not in {'telegram', 'slack', 'discord'} or not target:
        return False
    now = time.time() if now is None else now
    target = str(target)
    available = await asyncio.to_thread(check_available_update, runtime, now=now)
    if not available.has_update or not available.release:
        return False
    if not await asyncio.to_thread(_claim_notice, runtime, available.release, channel, target, now):
        return False
    try:
        sent = await send(target, update_notice_text(available, channel=channel))
        status = 'sent' if sent else 'unknown'
    except asyncio.CancelledError:
        # The server may have accepted the notice. Keep its claim on shutdown.
        raise
    except Exception as exc:
        status = replay_safety(exc)
        logger.warning('Update notice delivery %s (%s)', status, channel)
    await asyncio.to_thread(_finish_notice, runtime, available.release, channel, target, status, now)
    return status == 'sent'


async def run_update_notification_loop(runtime, *, channel, target, send,
                                       initial_delay=30, poll_interval=POLL_INTERVAL,
                                       replay_safety=send_error_replay_safety):
    """Target is a callable so a newly linked operator can receive a notice."""
    await asyncio.sleep(initial_delay)
    while True:
        try:
            await deliver_update_notice(runtime, channel=channel, target=target(), send=send,
                                        replay_safety=replay_safety)
        except Exception:
            logger.warning('Could not check for an update notice (%s)', channel, exc_info=True)
        await asyncio.sleep(poll_interval)
