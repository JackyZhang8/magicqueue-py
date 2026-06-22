"""基于 Redis List 的优先级队列驱动，适用于生产与分布式部署。

每个逻辑队列拆成三个 List（key 加后缀 ``:p2/:p1/:p0``）。消费使用多 key 的
``BLPOP``（按 高 -> 普通 -> 低 排列），既保证优先级又是阻塞消费，
不再使用 ``LLEN`` 轮询 + ``LPOP``。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .driver import Driver, QueueItem
from .priority import level_sub_key, ordered_keys, priority_level

if TYPE_CHECKING:
    import redis as _redis


class RedisDriver(Driver):
    """Redis 优先级队列驱动。"""

    def __init__(self, client: _redis.Redis) -> None:
        """复用调用方已配置好的 ``redis.Redis`` 客户端。"""
        self._client = client

    @classmethod
    def from_url(cls, url: str) -> RedisDriver:
        """根据连接串创建驱动并测试连通性，例如 ``redis://127.0.0.1:6379/0``。"""
        import redis  # 延迟导入，未用 Redis 时无需安装该依赖

        client = redis.Redis.from_url(url)
        client.ping()
        return cls(client)

    def push(self, queue_key: str, message: bytes, priority: int) -> None:
        key = level_sub_key(queue_key, priority_level(priority))
        self._client.rpush(key, message)

    def push_batch(self, items: list[QueueItem]) -> None:
        pipe = self._client.pipeline(transaction=False)
        for it in items:
            key = level_sub_key(it.queue_key, priority_level(it.priority))
            pipe.rpush(key, it.message)
        pipe.execute()

    def pop(self, queue_key: str) -> bytes | None:
        for key in ordered_keys(queue_key):
            val = self._client.lpop(key)
            if val is not None:
                return val
        return None

    def supports_blocking(self) -> bool:
        return True

    def bpop(self, queue_key: str, timeout: float) -> bytes | None:
        # BLPOP 返回 (key, value)，超时返回 None。
        res = self._client.blpop(ordered_keys(queue_key), timeout=timeout)
        if res is None:
            return None
        _key, value = res
        return value

    def size(self, queue_key: str) -> int:
        pipe = self._client.pipeline(transaction=False)
        for key in ordered_keys(queue_key):
            pipe.llen(key)
        return sum(int(n) for n in pipe.execute())

    def clear(self, queue_key: str) -> None:
        self._client.delete(*ordered_keys(queue_key))

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - close 出错不应影响关闭流程
            pass
