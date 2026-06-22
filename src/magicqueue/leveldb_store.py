"""基于 LevelDB 的持久化存储，用于崩溃恢复。

与 Go 版（goleveldb）、Rust 版（sled）的嵌入式 KV 持久层对齐。
依赖 `plyvel <https://plyvel.readthedocs.io/>`_（``pip install magicqueue[leveldb]``）；
在 Linux/macOS 上 ``plyvel`` 提供预编译 wheel，无需系统级 ``libleveldb``。
"""

from __future__ import annotations

import threading
from collections.abc import Iterable

from .persistence import Store


class LevelDBStore(Store):
    """基于 LevelDB 的键值持久层：id -> 消息 JSON 字节。

    与 sqlite 版语义一致：``put`` 覆盖写、``all`` 用于启动恢复。
    LevelDB 自身支持并发读写，这里仍对批量写加锁以保证
    ``put_batch``/``delete_batch`` 的原子性语义与 sqlite 对齐。
    """

    def __init__(self, path: str) -> None:
        try:
            import plyvel
        except ImportError as e:  # pragma: no cover - 取决于运行环境
            raise ImportError(
                "LevelDB persistence requires the 'plyvel' package; "
                'install it with: pip install "magicqueue[leveldb]"'
            ) from e

        # create_if_missing 对齐 LevelDB OpenFile / sled::open 的“自动创建”行为。
        self._db = plyvel.DB(path, create_if_missing=True)
        self._lock = threading.Lock()

    def put(self, msg_id: str, data: bytes) -> None:
        self._db.put(msg_id.encode("utf-8"), data)

    def put_batch(self, items: Iterable[tuple[str, bytes]]) -> None:
        with self._lock, self._db.write_batch() as wb:
            for msg_id, data in items:
                wb.put(msg_id.encode("utf-8"), data)

    def delete(self, msg_id: str) -> None:
        self._db.delete(msg_id.encode("utf-8"))

    def delete_batch(self, ids: Iterable[str]) -> None:
        with self._lock, self._db.write_batch() as wb:
            for msg_id in ids:
                wb.delete(msg_id.encode("utf-8"))

    def all(self) -> list[tuple[str, bytes]]:
        return [(key.decode("utf-8"), value) for key, value in self._db]

    def close(self) -> None:
        self._db.close()
