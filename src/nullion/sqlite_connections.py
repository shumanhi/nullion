"""Short-lived SQLite transactions with deterministic connection ownership."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import sqlite3
from typing import Any


@contextmanager
def sqlite_connection(*args: Any, **kwargs: Any) -> Iterator[sqlite3.Connection]:
    """Commit/rollback like Connection's context manager, then always close.

    SQLite's native context manager owns the transaction, not the handle.
    Keep those transaction semantics while releasing descriptors on returns,
    exceptions and commit failures, without waiting for garbage collection.
    """
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection as transaction:
            yield transaction
    finally:
        connection.close()
