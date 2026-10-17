"""Durable update progress; observing an operation never starts another update."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
from uuid import uuid4


class UpdateOperations:
    def __init__(self, path: Path):
        self.path = Path(path)
        if self.path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"}:
            self.path = self.path.with_suffix(".update-operations.db")

    @contextmanager
    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("""CREATE TABLE IF NOT EXISTS update_operations (
            operation_id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL,
            owner_pid INTEGER NOT NULL, status TEXT NOT NULL, events TEXT NOT NULL)""")
        connection.commit()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def start(self, request_key: str) -> tuple[str, bool]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM update_operations WHERE request_key=?", (request_key,)).fetchone()
            if row:
                self._reconcile(connection, row)
                return row["operation_id"], False
            row = connection.execute("SELECT * FROM update_operations WHERE status='running' LIMIT 1").fetchone()
            if row and not self._reconcile(connection, row):
                return row["operation_id"], False
            operation_id = uuid4().hex
            connection.execute("INSERT INTO update_operations VALUES (?,?,?,'running','[]')",
                               (operation_id, request_key, os.getpid()))
            return operation_id, True

    @staticmethod
    def _reconcile(connection, row) -> bool:
        if row["status"] != "running":
            return False
        try:
            os.kill(row["owner_pid"], 0)
        except ProcessLookupError:
            events = json.loads(row["events"])
            events.append({"step": "complete", "success": False, "rolled_back": False,
                           "error": "The update process stopped before recording its outcome. Check the installed version before retrying."})
            connection.execute("UPDATE update_operations SET status='interrupted',events=? WHERE operation_id=?",
                               (json.dumps(events), row["operation_id"]))
            return True
        except PermissionError:
            pass
        return False

    def append(self, operation_id: str, event: dict, *, complete: bool = False) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT events FROM update_operations WHERE operation_id=?", (operation_id,)).fetchone()
            if row is None:
                raise KeyError(operation_id)
            events = json.loads(row["events"])
            events.append(event)
            connection.execute("UPDATE update_operations SET events=?,status=? WHERE operation_id=?",
                               (json.dumps(events), "completed" if complete else "running", operation_id))

    def read(self, operation_id: str) -> dict | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM update_operations WHERE operation_id=?", (operation_id,)).fetchone()
            if row and self._reconcile(connection, row):
                row = connection.execute("SELECT * FROM update_operations WHERE operation_id=?", (operation_id,)).fetchone()
        return {"status": row["status"], "events": json.loads(row["events"])} if row else None
