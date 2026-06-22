"""基于 sqlite3 的持久化存储，用于崩溃恢复。"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterable


class SqliteStore:
    """简单的键值持久层：id -> 消息 JSON 字节。线程安全（内部加锁）。"""

    def __init__(self, path: str) -> None:
        # 自动创建父目录（与 LevelDB OpenFile 行为对齐）。
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # check_same_thread=False 配合显式锁，允许多线程共享同一连接。
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, data BLOB NOT NULL)"
            )
            self._conn.commit()

    def put(self, msg_id: str, data: bytes) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO messages (id, data) VALUES (?, ?)", (msg_id, data)
            )
            self._conn.commit()

    def put_batch(self, items: Iterable[tuple[str, bytes]]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO messages (id, data) VALUES (?, ?)", list(items)
            )
            self._conn.commit()

    def delete(self, msg_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
            self._conn.commit()

    def delete_batch(self, ids: Iterable[str]) -> None:
        with self._lock:
            self._conn.executemany(
                "DELETE FROM messages WHERE id = ?", [(i,) for i in ids]
            )
            self._conn.commit()

    def all(self) -> list[tuple[str, bytes]]:
        with self._lock:
            cur = self._conn.execute("SELECT id, data FROM messages")
            return [(row[0], row[1]) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
