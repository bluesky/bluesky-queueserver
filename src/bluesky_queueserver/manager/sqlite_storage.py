import asyncio
import os
import sqlite3
import threading
from collections.abc import Callable
from typing import ParamSpec, TypeVar

import platformdirs

_P = ParamSpec("_P")
_T = TypeVar("_T")


def get_default_sqlite_path() -> str:
    """
    Returns the default path to SQLite database file located in the OS-specific user cache
    directory. The directory is created if it does not exist.
    """
    cache_dir = platformdirs.user_cache_dir("bluesky-queueserver", appauthor=False)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "qserver.sqlite")


def _normalize_range(n: int, start: int, end: int) -> tuple[int, int]:
    """
    Convert Redis-style inclusive ``start``/``end`` indices (which may be negative) into
    non-negative indices for a list of length ``n``. The range is empty if ``start > end``.
    """
    if start < 0:
        start = max(n + start, 0)
    if end < 0:
        end = n + end
    end = min(end, n - 1)
    return start, end


class SQLiteStorage:
    """
    Persistent key-value and list storage based on SQLite. The class implements the subset
    of ``redis.asyncio.Redis`` API used by ``PlanQueueOperations`` with the same semantics,
    so it can be used in place of the connection to Redis server. Each method call is executed
    as a single transaction, so the operations are atomic, similarly to Redis commands.

    Parameters
    ----------
    path: str
        Path to the database file. The file is created if it does not exist.
    """

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        conn = sqlite3.connect(os.path.expanduser(path), check_same_thread=False, isolation_level=None)
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        # Positions are kept unique per key by the code; the index is non-unique so that
        #   positions can be shifted with a single UPDATE statement.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lists (key TEXT NOT NULL, pos INTEGER NOT NULL, value TEXT NOT NULL)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS lists_key_pos ON lists (key, pos)")
        self._conn: sqlite3.Connection | None = conn

    @property
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SQLite storage is closed")
        return self._conn

    async def _run(self, func: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
        def run_transaction() -> _T:
            with self._lock:
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    result = func(*args, **kwargs)
                except BaseException:
                    self._db.execute("ROLLBACK")
                    raise
                self._db.execute("COMMIT")
                return result

        return await asyncio.to_thread(run_transaction)

    def _llen(self, name: str) -> int:
        return self._db.execute("SELECT COUNT(*) FROM lists WHERE key = ?", (name,)).fetchone()[0]

    def _pos_at(self, name: str, index: int) -> int:
        row = self._db.execute(
            "SELECT pos FROM lists WHERE key = ? ORDER BY pos LIMIT 1 OFFSET ?", (name, index)
        ).fetchone()
        return row[0]

    async def ping(self) -> bool:
        def ping() -> bool:
            self._db.execute("SELECT 1").fetchone()
            return True

        return await self._run(ping)

    async def aclose(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    async def get(self, name: str) -> str | None:
        def get() -> str | None:
            row = self._db.execute("SELECT value FROM kv WHERE key = ?", (name,)).fetchone()
            return row[0] if row else None

        return await self._run(get)

    async def set(self, name: str, value: str) -> bool:
        def set_() -> bool:
            self._db.execute("DELETE FROM lists WHERE key = ?", (name,))
            self._db.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", (name, value))
            return True

        return await self._run(set_)

    async def delete(self, *names: str) -> int:
        def delete() -> int:
            n_deleted = 0
            for name in names:
                n_kv = self._db.execute("DELETE FROM kv WHERE key = ?", (name,)).rowcount
                n_list = self._db.execute("DELETE FROM lists WHERE key = ?", (name,)).rowcount
                n_deleted += 1 if (n_kv or n_list) else 0
            return n_deleted

        return await self._run(delete)

    async def llen(self, name: str) -> int:
        return await self._run(self._llen, name)

    async def lrange(self, name: str, start: int, end: int) -> list[str]:
        def lrange() -> list[str]:
            start_, end_ = _normalize_range(self._llen(name), start, end)
            if start_ > end_:
                return []
            rows = self._db.execute(
                "SELECT value FROM lists WHERE key = ? ORDER BY pos LIMIT ? OFFSET ?",
                (name, end_ - start_ + 1, start_),
            ).fetchall()
            return [_[0] for _ in rows]

        return await self._run(lrange)

    async def lindex(self, name: str, index: int) -> str | None:
        def lindex() -> str | None:
            n = self._llen(name)
            ind = n + index if index < 0 else index
            if not (0 <= ind < n):
                return None
            row = self._db.execute(
                "SELECT value FROM lists WHERE key = ? ORDER BY pos LIMIT 1 OFFSET ?", (name, ind)
            ).fetchone()
            return row[0]

        return await self._run(lindex)

    def _push(self, name: str, values: tuple[str, ...], *, left: bool) -> int:
        for value in values:
            if left:
                pos = self._db.execute("SELECT MIN(pos) - 1 FROM lists WHERE key = ?", (name,)).fetchone()[0]
            else:
                pos = self._db.execute("SELECT MAX(pos) + 1 FROM lists WHERE key = ?", (name,)).fetchone()[0]
            pos = 0 if pos is None else pos
            self._db.execute("INSERT INTO lists (key, pos, value) VALUES (?, ?, ?)", (name, pos, value))
        return self._llen(name)

    async def lpush(self, name: str, *values: str) -> int:
        return await self._run(self._push, name, values, left=True)

    async def rpush(self, name: str, *values: str) -> int:
        return await self._run(self._push, name, values, left=False)

    def _pop(self, name: str, *, left: bool) -> str | None:
        order = "ASC" if left else "DESC"
        row = self._db.execute(
            f"SELECT rowid, value FROM lists WHERE key = ? ORDER BY pos {order} LIMIT 1", (name,)
        ).fetchone()
        if row is None:
            return None
        self._db.execute("DELETE FROM lists WHERE rowid = ?", (row[0],))
        return row[1]

    async def lpop(self, name: str) -> str | None:
        return await self._run(self._pop, name, left=True)

    async def rpop(self, name: str) -> str | None:
        return await self._run(self._pop, name, left=False)

    async def lrem(self, name: str, count: int, value: str) -> int:
        def lrem() -> int:
            if count == 0:
                return self._db.execute("DELETE FROM lists WHERE key = ? AND value = ?", (name, value)).rowcount
            order = "ASC" if count > 0 else "DESC"
            rows = self._db.execute(
                f"SELECT rowid FROM lists WHERE key = ? AND value = ? ORDER BY pos {order} LIMIT ?",
                (name, value, abs(count)),
            ).fetchall()
            self._db.executemany("DELETE FROM lists WHERE rowid = ?", rows)
            return len(rows)

        return await self._run(lrem)

    async def linsert(self, name: str, where: str, refvalue: str, value: str) -> int:
        def linsert() -> int:
            if self._llen(name) == 0:
                return 0
            row = self._db.execute(
                "SELECT pos FROM lists WHERE key = ? AND value = ? ORDER BY pos LIMIT 1", (name, refvalue)
            ).fetchone()
            if row is None:
                return -1
            pivot: int = row[0]
            if where.upper() == "BEFORE":
                self._db.execute("UPDATE lists SET pos = pos + 1 WHERE key = ? AND pos >= ?", (name, pivot))
                pos = pivot
            elif where.upper() == "AFTER":
                self._db.execute("UPDATE lists SET pos = pos + 1 WHERE key = ? AND pos > ?", (name, pivot))
                pos = pivot + 1
            else:
                raise ValueError(f"Parameter 'where' must be 'BEFORE' or 'AFTER': {where!r}")
            self._db.execute("INSERT INTO lists (key, pos, value) VALUES (?, ?, ?)", (name, pos, value))
            return self._llen(name)

        return await self._run(linsert)

    async def ltrim(self, name: str, start: int, end: int) -> bool:
        def ltrim() -> bool:
            start_, end_ = _normalize_range(self._llen(name), start, end)
            if start_ > end_:
                self._db.execute("DELETE FROM lists WHERE key = ?", (name,))
            else:
                pos_start, pos_end = self._pos_at(name, start_), self._pos_at(name, end_)
                self._db.execute(
                    "DELETE FROM lists WHERE key = ? AND (pos < ? OR pos > ?)", (name, pos_start, pos_end)
                )
            return True

        return await self._run(ltrim)
