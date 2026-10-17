"""Durable delivery plans and receipts, separate from completed agent work."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from nullion.entrypoint_guard import SingleInstanceLock


class DeliveryOutbox:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path is not None else None
        if self.path is not None and self.path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
            self.path = self.path.with_suffix(".delivery-outbox.db")
        self._memory: dict[str, dict] = {}

    @contextmanager
    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("""CREATE TABLE IF NOT EXISTS delivery_outbox (
            group_id TEXT PRIMARY KEY, payload TEXT NOT NULL)""")
        connection.commit()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def get(self, group_id: str) -> dict | None:
        if self.path is None:
            record = self._memory.get(group_id)
            return json.loads(json.dumps(record)) if record else None
        with self._connect() as connection:
            row = connection.execute("SELECT payload FROM delivery_outbox WHERE group_id=?", (group_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, group_id: str, record: dict) -> None:
        if self.path is None:
            self._memory[group_id] = json.loads(json.dumps(record))
            return
        with self._connect() as connection:
            connection.execute("INSERT OR REPLACE INTO delivery_outbox VALUES (?,?)", (group_id, json.dumps(record)))

    def enqueue(self, group_id: str, record: dict) -> None:
        """Preparing the same group twice must never erase delivery receipts."""
        if self.path is None:
            self._memory.setdefault(group_id, json.loads(json.dumps(record)))
            return
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO delivery_outbox VALUES (?,?)", (group_id, json.dumps(record)))

    def due(self, *, surfaces: tuple[str, ...] | None = None) -> list[str]:
        if self.path is None:
            rows = self._memory.items()
        else:
            with self._connect() as connection:
                query = "SELECT group_id,payload FROM delivery_outbox WHERE json_extract(payload,'$.status') IN ('pending','failed','delivering') AND json_extract(payload,'$.attempts')<3 AND json_extract(payload,'$.retry_at')<=?"
                parameters: list = [time.time()]
                if surfaces is not None:
                    if not surfaces:
                        return []
                    query += " AND substr(json_extract(payload,'$.conversation_id'),1,instr(json_extract(payload,'$.conversation_id'),':')-1) IN (" + ",".join("?" for _ in surfaces) + ")"
                    parameters.extend(surfaces)
                rows = [(row[0], json.loads(row[1])) for row in connection.execute(query + " LIMIT 20", parameters)]
        return [key for key, record in rows if record.get("status") != "delivered"
                and (surfaces is None or record.get("conversation_id", "").partition(":")[0] in surfaces)
                and record.get("attempts", 0) < 3 and record.get("retry_at", 0) <= time.time()][:20]

    @contextmanager
    def claim(self, group_id: str):
        if self.path is None:
            yield True
            return
        lock = SingleInstanceLock(hashlib.sha256(group_id.encode()).hexdigest(),
                                  lock_dir=self.path.parent / ".delivery-locks")
        acquired = lock.acquire()
        try:
            yield acquired
        finally:
            if acquired:
                lock.release()
