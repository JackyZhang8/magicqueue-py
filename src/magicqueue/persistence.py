"""持久化存储抽象与基于 sqlite3 的默认实现，用于崩溃恢复。"""

from __future__ import annotations

import os
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable


class Store(ABC):
    """持久层抽象：id -> 消息 JSON 字节的键值存储。

    实现需保证线程安全（多个 worker/消费者线程会并发访问）。
    内置 :class:`SqliteStore`（零额外依赖）与
    :class:`~magicqueue.leveldb_store.LevelDBStore`（与 Go/Rust 版的
    LevelDB/sled 对齐，需 ``pip install magicqueue[leveldb]``）。
    """

    @abstractmethod
    def put(self, msg_id: str, data: bytes) -> None:
        """写入或覆盖一条消息。"""

    @abstractmethod
    def put_batch(self, items: Iterable[tuple[str, bytes]]) -> None:
        """在一次原子操作中批量写入。"""

    @abstractmethod
    def delete(self, msg_id: str) -> None:
        """删除一条消息（已确认）。"""

    @abstractmethod
    def delete_batch(self, ids: Iterable[str]) -> None:
        """批量删除消息。"""

    @abstractmethod
    def all(self) -> list[tuple[str, bytes]]:
        """返回全部 (id, data)，用于启动时崩溃恢复。"""

    @abstractmethod
    def close(self) -> None:
        """释放底层资源。"""


class SqliteStore(Store):
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
